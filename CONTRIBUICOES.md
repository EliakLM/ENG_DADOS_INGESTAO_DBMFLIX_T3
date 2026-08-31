# Registro de Contribuições

## Grupo: Data Engineers — T3

| Membro | Matrícula | Responsabilidade Principal |
|--------|-----------|----------------------------|
| Eliak Lima | XXXXXX | `src/utils.py`, `src/extractor.py`, `config/`, `notebooks/00_setup_secrets.py`, evidências de execução |
| Renan Madeira | XXXXXX | `src/loader.py`, `src/control.py`, `notebooks/01_run_pipeline.py`, `docs/ARQUITETURA.md` |
| Raul Carvalho Teles | 2650364 | `notebooks/02_validate_bronze.py`, `tests/`, `jobs/ingestion_workflow.json`, `README.md`, `CONTRIBUICOES.md` |

## Divisão Técnica por Componente

### Eliak Lima — Extração & Infraestrutura

| Artefato | Descrição |
|----------|-----------|
| `notebooks/00_setup_secrets.py` | Criação do secret scope `conn-db` e armazenamento da connection string do MongoDB Atlas no Databricks |
| `config/pipeline_config.yaml` | Configuração global da pipeline (catálogo, schema, paths, limiar de qualidade) |
| `config/collections.json` | Parâmetros por coleção: modo de carga, watermark field, projection, batch size |
| `src/utils.py` | Utilitários: `encode_bson`, `retry_with_backoff` com exponential backoff, `generate_run_id`, `hash_document` |
| `src/extractor.py` | `MongoExtractor`: leitura paginada via cursor (sem `list(cursor)`), pushdown de projection, connection pooling, escrita JSONL na landing zone |
| `docs/evidencias/` | Screenshots das 3 execuções obrigatórias |

**Cobertura de requisitos:** R1 (pipeline genérica), R2 (boas práticas de recursos)

---

### Renan Madeira — Bronze Layer & Controle

| Artefato | Descrição |
|----------|-----------|
| `src/control.py` | `IngestionControl`: criação das tabelas `watermark_store` e `control_ingestion_log`, leitura/escrita de watermark via MERGE, log de execuções |
| `src/loader.py` | `BronzeLoader`: Auto Loader com `cloudFiles`, `mergeSchema`, `_rescue_data`, particionamento por `_ingestion_date`, colunas de rastreabilidade R4 |
| `notebooks/01_run_pipeline.py` | Orquestrador principal: loop sobre coleções, integração extractor + loader + control, reconciliação de qualidade |
| `docs/ARQUITETURA.md` | Decisões técnicas documentadas: formato, trigger, idempotência, schema drift |

**Cobertura de requisitos:** R3 (modos de carga + idempotência), R4 (rastreabilidade), R5 (control_ingestion_log), R6 (Bronze fiel), R7 (schema drift)

---

### Raul Teles — Qualidade, Bônus & Documentação

| Artefato | Descrição |
|----------|-----------|
| `notebooks/02_validate_bronze.py` | Validações R8: reconciliação, nulos em `_source_id`, duplicatas por lote, dashboard de observabilidade |
| `tests/test_utils.py` | Testes unitários de `encode_bson`, `retry_with_backoff` com mocks |
| `tests/test_control.py` | Testes de `get_watermark`, `log_start`, `log_end` com SparkSession mockada |
| `tests/test_extractor.py` | Testes de `MongoExtractor` com `MongoClient` mockado |
| `jobs/ingestion_workflow.json` | Workflow Databricks: agendamento diário, retry automático, notificação por e-mail em falha |
| `README.md` | Documentação completa: arquitetura, decisões técnicas, como executar, limitações |
| `CONTRIBUICOES.md` | Este arquivo |

**Cobertura de requisitos:** R8 (reconciliação), Bônus Orquestração (+4), Bônus Observabilidade (+3), Bônus Testes (+3)

---

## Detalhamento por commit

> Cole aqui a saída de `git log --oneline --author="Nome"` para cada membro após a finalização:

```
# Eliak Lima
git log --oneline --author="Eliak"

# Renan Madeira
git log --oneline --author="Renan"

# Raul Teles
git log --oneline --author="Raul"
```
