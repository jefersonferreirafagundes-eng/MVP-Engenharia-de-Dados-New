# Databricks notebook source
# MVP Engenharia de Dados
# ETAPA 01 - Ingestão da camada Bronze

from pyspark.sql import functions as F

SOURCE_PATH = (
    "/Volumes/workspace/mvp_raw/input_files/"
    "student_productivity_distraction_dataset_20000.csv"
)

BRONZE_TABLE = "workspace.mvp_bronze.student_productivity_raw"

print("Arquivo de origem:")
print(SOURCE_PATH)

print("\nTabela de destino:")
print(BRONZE_TABLE)

# COMMAND ----------

# Leitura inicial do arquivo CSV
# Nesta etapa ainda não alteramos nem persistimos os dados.

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "PERMISSIVE")
    .csv(SOURCE_PATH)
)

# Quantidade de registros e colunas
total_linhas = df_raw.count()
total_colunas = len(df_raw.columns)

print(f"Quantidade de registros: {total_linhas}")
print(f"Quantidade de colunas: {total_colunas}")

print("\nColunas encontradas:")
for coluna in df_raw.columns:
    print("-", coluna)

# COMMAND ----------

# Validação do dataset antes da persistência na Bronze

print("=== RESUMO DO DATASET BRUTO ===")
print(f"Quantidade de registros: {df_raw.count()}")
print(f"Quantidade de colunas: {len(df_raw.columns)}")

print("\n=== SCHEMA ORIGINAL ===")
df_raw.printSchema()

print("\n=== PRIMEIROS REGISTROS ===")
display(df_raw.limit(10))

# COMMAND ----------

# Persistência da camada Bronze
# Mantemos os dados como recebidos e adicionamos apenas metadados de rastreabilidade.

from pyspark.sql import functions as F

df_bronze = (
    df_raw
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

print("Registros antes da gravação:", df_bronze.count())
print("Colunas após inclusão dos metadados:", len(df_bronze.columns))

display(df_bronze.limit(10))

# COMMAND ----------

BRONZE_TABLE = "workspace.mvp_bronze.student_productivity_raw"

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print("Tabela Bronze persistida com sucesso:")
print(BRONZE_TABLE)

# COMMAND ----------

df_bronze_check = spark.table(BRONZE_TABLE)

print("=== VALIDAÇÃO DA BRONZE ===")
print("Tabela:", BRONZE_TABLE)
print("Quantidade de registros:", df_bronze_check.count())
print("Quantidade de colunas:", len(df_bronze_check.columns))

display(df_bronze_check.limit(10))