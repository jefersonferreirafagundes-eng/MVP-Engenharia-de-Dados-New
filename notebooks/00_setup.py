# Databricks notebook source
# MVP Engenharia de Dados
# ETAPA 00 - Configuração da arquitetura do projeto

# Identifica automaticamente o catálogo disponível
CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).first()["catalog"]

# Schemas da arquitetura Medalhão
RAW_SCHEMA = "mvp_raw"
BRONZE_SCHEMA = "mvp_bronze"
SILVER_SCHEMA = "mvp_silver"
GOLD_SCHEMA = "mvp_gold"

print("Catálogo utilizado:", CATALOG)

# Criação dos schemas
for schema in [
    RAW_SCHEMA,
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA
]:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`"
    )
    print(f"Schema criado/verificado: {schema}")

print("\nEstrutura inicial do MVP criada com sucesso.")