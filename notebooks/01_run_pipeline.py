# Databricks notebook source
# =============================================================================
# 01_run_pipeline.py
# -----------------------------------------------------------------------------
# Orquestrador principal da pipeline de ingestão sample_mflix → Bronze.
#
# Fluxo por coleção:
#   1. Lê configuração de config/pipeline_config.yaml e config/collections.json
#   2. Para cada coleção:
#      a. Obtém watermark anterior (incremental) ou ignora (full)
#      b. Extrai do MongoDB → grava JSONL na Landing Zone
#      c. Carrega da Landing → Bronze Delta com Auto Loader
#      d. Reconcilia contagens (R8)
#      e. Salva watermark e registra log de execução
#
# Responsável: Renan Madeira
# =============================================================================

# COMMAND ----------

# DBTITLE 1, Install dependencies
# MAGIC %pip install pymongo pyyaml

# COMMAND ----------

# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1, Imports
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

# Localiza a raiz tanto em Git Folders interativos quanto em Jobs com Git source.
current_path = Path.cwd().resolve()
repo_candidates = (current_path, *current_path.parents)
repo_root = next(
    (
        path
        for path in repo_candidates
        if (path / "config" / "pipeline_config.yaml").is_file()
        and (path / "src" / "extractor.py").is_file()
    ),
    None,
)
if repo_root is None:
    raise RuntimeError(
        "Raiz do repositório não encontrada a partir do diretório atual: "
        f"{current_path}"
    )

REPO_ROOT = str(repo_root)
sys.path.insert(0, REPO_ROOT)

from src.extractor import MongoExtractor
from src.loader import BronzeLoader
from src.control import IngestionControl
from src.utils import generate_run_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("pipeline.orchestrator")

# COMMAND ----------

# DBTITLE 1, Load configuration
CONFIG_PATH = Path(REPO_ROOT) / "config" / "pipeline_config.yaml"
COLLECTIONS_PATH = Path(REPO_ROOT) / "config" / "collections.json"

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

with open(COLLECTIONS_PATH) as f:
    collections_cfg = json.load(f)["collections"]

logger.info("Configuração carregada: catalog=%s, landing=%s", config["catalog"], config["landing_path"])
logger.info("Coleções configuradas: %s", [c["name"] for c in collections_cfg])

# COMMAND ----------

# DBTITLE 1, Initialize components
spark = SparkSession.builder.getOrCreate()

run_id = generate_run_id()
run_start = datetime.now(timezone.utc)

logger.info("="*60)
logger.info("INÍCIO DA EXECUÇÃO | run_id=%s | %s", run_id, run_start.isoformat())
logger.info("="*60)

extractor = MongoExtractor(config=config, dbutils=dbutils)
loader = BronzeLoader(spark=spark, config=config)
control = IngestionControl(
    spark=spark,
    catalog=config["catalog"],
    schema=config["bronze_schema"]
)

# COMMAND ----------

# DBTITLE 1, Execute pipeline for each collection
quality_threshold = config.get("quality_threshold_pct", 1.0)
pipeline_results = []

for col_cfg in collections_cfg:
    collection = col_cfg["name"]
    load_type = col_cfg["load_type"]
    watermark_field = col_cfg.get("watermark_field")

    logger.info("-"*50)
    logger.info("Processando coleção: %s | modo: %s", collection, load_type)

    col_start = datetime.now(timezone.utc)
    watermark_ini = None
    watermark_final = None
    status = "FAILED"
    erro = None
    qtd_lida = 0
    qtd_gravada = 0

    try:
        # --- Passo 1: Obtém watermark anterior (apenas incremental) ---
        if load_type == "incremental":
            watermark_ini = control.get_watermark(collection)
            logger.info("Watermark anterior: %s", watermark_ini)

        # Registra início no log de controle
        control.log_start(
            run_id=run_id,
            collection=collection,
            load_type=load_type,
            watermark_ini=watermark_ini,
            start_time=col_start
        )

        # --- Passo 2: Extrai do MongoDB → Landing Zone ---
        logger.info("Iniciando extração do MongoDB...")
        qtd_lida = extractor.extract(
            collection_cfg=col_cfg,
            run_id=run_id,
            watermark=watermark_ini
        )
        logger.info("Extração concluída: %d documentos lidos", qtd_lida)

        if qtd_lida == 0 and load_type == "incremental":
            logger.info("Nenhum dado novo desde a última watermark — encerrando coleção com sucesso.")
            status = "SUCCESS"
            watermark_final = watermark_ini
            pipeline_results.append({"collection": collection, "status": status, "qtd_lida": 0, "qtd_gravada": 0})
            continue

        # --- Passo 3: Carrega Landing → Bronze Delta ---
        logger.info("Iniciando carga para Bronze (Auto Loader)...")
        qtd_gravada = loader.load(
            collection=collection,
            run_id=run_id,
            load_type=load_type
        )
        logger.info("Carga concluída: %d documentos gravados", qtd_gravada)

        # --- Passo 4: Reconciliação de qualidade (R8) ---
        if qtd_lida > 0:
            divergencia_pct = abs(qtd_lida - qtd_gravada) / qtd_lida * 100
        else:
            divergencia_pct = 0.0

        if divergencia_pct > quality_threshold:
            status = "PARTIAL"
            erro = (
                f"Divergência de {divergencia_pct:.2f}% entre origem ({qtd_lida}) "
                f"e destino ({qtd_gravada}) — limiar: {quality_threshold}%"
            )
            logger.warning("⚠️  %s", erro)
        else:
            status = "SUCCESS"
            logger.info("✅ Reconciliação OK: divergência %.2f%%", divergencia_pct)

        # --- Passo 5: Salva watermark (apenas incremental com dados) ---
        if load_type == "incremental" and qtd_lida > 0:
            watermark_final = extractor.get_max_watermark(col_cfg, doc_filter={})
            if watermark_final:
                control.save_watermark(collection, watermark_final)
                logger.info("Watermark atualizada: %s → %s", watermark_ini, watermark_final)

    except Exception as e:
        status = "FAILED"
        erro = str(e)
        logger.error("❌ Falha ao processar %s: %s", collection, erro, exc_info=True)

    finally:
        col_end = datetime.now(timezone.utc)
        control.log_end(
            run_id=run_id,
            collection=collection,
            qtd_lida=qtd_lida,
            qtd_gravada=qtd_gravada,
            status=status,
            watermark_final=watermark_final,
            start_time=col_start,
            end_time=col_end,
            erro=erro
        )

    pipeline_results.append({
        "collection": collection,
        "status": status,
        "qtd_lida": qtd_lida,
        "qtd_gravada": qtd_gravada,
        "erro": erro
    })

extractor.close()

# COMMAND ----------

# DBTITLE 1, Pipeline summary
run_end = datetime.now(timezone.utc)
duracao_total = (run_end - run_start).total_seconds()

logger.info("="*60)
logger.info("FIM DA EXECUÇÃO | run_id=%s | duração: %.1fs", run_id, duracao_total)
logger.info("="*60)

print(f"\n{'='*60}")
print(f"  RESUMO DA EXECUÇÃO")
print(f"  run_id  : {run_id}")
print(f"  duração : {duracao_total:.1f}s")
print(f"{'='*60}")
print(f"{'Coleção':<20} {'Status':<10} {'Lidos':>8} {'Gravados':>10}")
print(f"{'-'*55}")
for r in pipeline_results:
    print(f"{r['collection']:<20} {r['status']:<10} {r['qtd_lida']:>8} {r.get('qtd_gravada', 0):>10}")
print(f"{'='*60}\n")

# COMMAND ----------

# DBTITLE 1, Show control log for this run
spark.sql(f"""
    SELECT collection, load_type, watermark_inicial, watermark_final,
           qtd_lida_origem, qtd_gravada_destino,
           duracao_seg, status, mensagem_erro
    FROM {config['catalog']}.{config['bronze_schema']}.control_ingestion_log
    WHERE _ingestion_id = '{run_id}'
    ORDER BY collection
""").display()
