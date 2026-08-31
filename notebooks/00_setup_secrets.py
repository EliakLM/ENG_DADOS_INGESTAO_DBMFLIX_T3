# Databricks notebook source
# =============================================================================
# 00_setup_secrets.py
# -----------------------------------------------------------------------------
# Cria o Secret Scope 'conn-db' e armazena a connection string do MongoDB.
#
# ⚠️  ATENÇÃO: Este notebook NÃO deve ser executado como parte da pipeline
#     automatizada. Ele é um utilitário de setup a ser rodado UMA VEZ por um
#     administrador com permissão de criação de secrets.
#
# ⚠️  NUNCA versione a connection string real neste arquivo.
#     Insira a URI real apenas ao executar manualmente no Databricks.
#
# Responsável: Eliak Lima
# =============================================================================

# COMMAND ----------

# DBTITLE 1, Step 1 — Criar Secret Scope 'conn-db'
import requests

instance = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}"}

payload = {
    "scope": "conn-db",
    "initial_manage_principal": "users"  # "users" = todos do workspace podem usar
}

response = requests.post(
    f"{instance}/api/2.0/secrets/scopes/create",
    headers=headers,
    json=payload
)

if response.status_code == 200:
    print("✅ Secret scope 'conn-db' criado com sucesso.")
elif "RESOURCE_ALREADY_EXISTS" in response.text:
    print("ℹ️  Secret scope 'conn-db' já existe — nenhuma ação necessária.")
else:
    print(f"❌ Erro ({response.status_code}): {response.text}")

# COMMAND ----------

# DBTITLE 1, Step 2 — Armazenar connection string do MongoDB
# ⚠️  Substitua o valor de MONGODB_URI pela connection string real antes de executar.
# ⚠️  Nunca faça commit deste arquivo com a URI real preenchida.

MONGODB_URI = "mongodb://USER:PASSWORD@HOST:PORT/?directConnection=true"  # <-- substitua aqui

if "USER:PASSWORD" in MONGODB_URI:
    raise ValueError(
        "⛔ Você esqueceu de substituir a connection string placeholder. "
        "Edite a variável MONGODB_URI antes de executar este cell."
    )

payload = {
    "scope": "conn-db",
    "key": "cnn-mongodb-sampleflix",
    "string_value": MONGODB_URI
}

response = requests.post(
    f"{instance}/api/2.0/secrets/put",
    headers=headers,
    json=payload
)

if response.status_code == 200:
    print("✅ Secret 'cnn-mongodb-sampleflix' criada/atualizada com sucesso!")
else:
    print(f"❌ Erro ({response.status_code}): {response.text}")

# COMMAND ----------

# DBTITLE 1, Step 3 — Verificar se o secret foi armazenado corretamente
# Testa que o secret pode ser lido (não exibe o valor, apenas confirma acesso)
try:
    secret_value = dbutils.secrets.get(scope="conn-db", key="cnn-mongodb-sampleflix")
    # Mostra apenas os primeiros 15 caracteres para confirmar que não está vazio
    preview = secret_value[:15] + "..." if len(secret_value) > 15 else "***"
    print(f"✅ Secret lido com sucesso. Preview: {preview}")
except Exception as e:
    print(f"❌ Falha ao ler o secret: {e}")

# COMMAND ----------

# DBTITLE 1,Step 4 — Testar conectividade com o MongoDB
%pip install pymongo

from pymongo import MongoClient

uri = dbutils.secrets.get(scope="conn-db", key="cnn-mongodb-sampleflix")

try:
    client = MongoClient(uri, serverSelectionTimeoutMS=10_000, appName="setup-test")
    collections = sorted(client["sample_mflix"].list_collection_names())
    print(f"✅ Conexão bem-sucedida! Coleções encontradas em sample_mflix: {collections}")
    client.close()
except Exception as e:
    print(f"❌ Falha na conexão com MongoDB: {e}")
