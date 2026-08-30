# Databricks notebook source
# =============================================================================
# 02_validate_bronze.py
# -----------------------------------------------------------------------------
# Validações de qualidade (R8) e observabilidade sobre a camada Bronze.
#
# Executa as seguintes verificações:
#   1. Contagem origem × destino por execução e acumulada
#   2. Percentual de nulos em _source_id por tabela
#   3. Duplicidade de _source_id dentro do mesmo _ingestion_id
#   4. Dashboard de observabilidade sobre control_ingestion_log
#
# Responsável: Raul Teles
# =============================================================================

# COMMAND ----------

# DBTITLE 1, Configuration
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "mflix_catalog"
SCHEMA = "bronze"
COLLECTIONS = ["movies", "comments", "users", "theaters", "sessions", "embedded_movies"]

spark = SparkSession.builder.getOrCreate()

print(f"Validando camada Bronze: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1, R8.1 — Contagem por execução (últimas 10 runs)
print("=" * 60)
print("R8.1 — Contagem por execução (últimas 10 runs)")
print("=" * 60)

spark.sql(f"""
    SELECT
        _ingestion_id,
        collection,
        load_type,
        qtd_lida_origem,
        qtd_gravada_destino,
        ROUND(ABS(qtd_lida_origem - qtd_gravada_destino) / NULLIF(qtd_lida_origem, 0) * 100, 2) AS divergencia_pct,
        status,
        ROUND(duracao_seg, 1) AS duracao_seg,
        DATE(start_time) AS data_execucao
    FROM {CATALOG}.{SCHEMA}.control_ingestion_log
    ORDER BY start_time DESC
    LIMIT 60
""").display()

# COMMAND ----------

# DBTITLE 1, R8.2 — Verificar nulos em _source_id por tabela
print("=" * 60)
print("R8.2 — Verificação de nulos em _source_id (jamais deve ser nulo)")
print("=" * 60)

null_results = []
for collection in COLLECTIONS:
    try:
        df = spark.table(f"{CATALOG}.{SCHEMA}.{collection}")
        total = df.count()
        if total == 0:
            null_results.append({"collection": collection, "total": 0, "nulls_source_id": 0, "pct_nulo": 0.0, "status": "OK (vazia)"})
            continue
        nulls = df.filter(F.col("_source_id").isNull() | (F.col("_source_id") == "UNKNOWN")).count()
        pct = round(nulls / total * 100, 4)
        status = "❌ FALHA" if nulls > 0 else "✅ OK"
        null_results.append({"collection": collection, "total": total, "nulls_source_id": nulls, "pct_nulo": pct, "status": status})
    except Exception as e:
        null_results.append({"collection": collection, "total": -1, "nulls_source_id": -1, "pct_nulo": -1.0, "status": f"ERRO: {e}"})

spark.createDataFrame(null_results).display()

# COMMAND ----------

# DBTITLE 1, R8.3 — Verificar duplicatas de _source_id dentro do mesmo lote
print("=" * 60)
print("R8.3 — Duplicatas de _source_id dentro do mesmo _ingestion_id")
print("=" * 60)

for collection in COLLECTIONS:
    try:
        df = spark.table(f"{CATALOG}.{SCHEMA}.{collection}")
        if df.count() == 0:
            print(f"  {collection}: vazia, pulando.")
            continue
        dups = (
            df.groupBy("_ingestion_id", "_source_id")
            .agg(F.count("*").alias("cnt"))
            .filter(F.col("cnt") > 1)
        )
        dup_count = dups.count()
        if dup_count > 0:
            print(f"  ⚠️  {collection}: {dup_count} combinações (_ingestion_id, _source_id) duplicadas")
            dups.show(10, truncate=False)
        else:
            print(f"  ✅  {collection}: sem duplicatas no mesmo lote")
    except Exception as e:
        print(f"  ❌  {collection}: erro — {e}")

# COMMAND ----------

# DBTITLE 1, Observabilidade — Volume ingerido por dia
print("=" * 60)
print("OBSERVABILIDADE — Volume ingerido por dia (últimos 30 dias)")
print("=" * 60)

spark.sql(f"""
    SELECT
        DATE(start_time)       AS data_execucao,
        collection,
        COUNT(*)               AS num_execucoes,
        SUM(qtd_gravada_destino) AS total_docs_gravados,
        ROUND(AVG(duracao_seg), 1) AS duracao_media_seg,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS sucessos,
        SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS falhas,
        SUM(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END) AS parciais
    FROM {CATALOG}.{SCHEMA}.control_ingestion_log
    WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY DATE(start_time), collection
    ORDER BY data_execucao DESC, collection
""").display()

# COMMAND ----------

# DBTITLE 1, Observabilidade — Taxa de falha por coleção (histórico completo)
spark.sql(f"""
    SELECT
        collection,
        COUNT(*)  AS total_runs,
        ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS taxa_sucesso_pct,
        ROUND(SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS taxa_falha_pct,
        ROUND(AVG(duracao_seg), 1) AS duracao_media_seg,
        MAX(DATE(start_time)) AS ultima_execucao
    FROM {CATALOG}.{SCHEMA}.control_ingestion_log
    GROUP BY collection
    ORDER BY collection
""").display()

# COMMAND ----------

# DBTITLE 1, Observabilidade — Watermarks atuais
spark.sql(f"""
    SELECT collection, watermark_value, updated_at
    FROM {CATALOG}.{SCHEMA}.watermark_store
    ORDER BY collection
""").display()

# COMMAND ----------

# DBTITLE 1, Observabilidade — Acumulado total por tabela Bronze
print("=" * 60)
print("ACUMULADO — Total de registros por tabela Bronze")
print("=" * 60)

totals = []
for collection in COLLECTIONS:
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{collection}").count()
        totals.append({"collection": collection, "total_registros": count})
    except Exception as e:
        totals.append({"collection": collection, "total_registros": -1})

spark.createDataFrame(totals).display()
