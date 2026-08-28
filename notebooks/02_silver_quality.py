# Databricks notebook source
# MVP Engenharia de Dados
# ETAPA 02 - Diagnóstico de qualidade e preparação da camada Silver

from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.mvp_bronze.student_productivity_raw"
SILVER_TABLE = "workspace.mvp_silver.student_productivity_clean"

df = spark.table(BRONZE_TABLE)

print("=== CAMADA BRONZE CARREGADA ===")
print("Registros:", df.count())
print("Colunas:", len(df.columns))

display(df.limit(10))

# COMMAND ----------

# Verificação de unicidade do identificador student_id

total_registros = df.count()
ids_distintos = df.select("student_id").distinct().count()

duplicados = total_registros - ids_distintos

print("=== ANÁLISE DE UNICIDADE ===")
print("Total de registros:", total_registros)
print("student_id distintos:", ids_distintos)
print("Possíveis duplicatas:", duplicados)

# COMMAND ----------

# Verificação de completude por atributo

print("=== ANÁLISE DE COMPLETUDE ===")

colunas_originais = [
    c for c in df.columns
    if not c.startswith("_")
]

resultado_nulos = []

for c in colunas_originais:
    nulos = df.filter(
        F.col(c).isNull() |
        (F.trim(F.col(c)) == "")
    ).count()

    percentual = (nulos / total_registros) * 100

    resultado_nulos.append(
        (c, nulos, round(percentual, 2))
    )

df_nulos = spark.createDataFrame(
    resultado_nulos,
    ["coluna", "valores_nulos", "percentual_nulos"]
)

display(df_nulos)

# COMMAND ----------

# Conversão dos tipos para preparação da camada Silver

integer_cols = [
    "student_id",
    "age",
    "breaks_per_day",
    "coffee_intake_mg",
    "exercise_minutes",
    "assignments_completed",
    "stress_level",
    "focus_score"
]

double_cols = [
    "study_hours_per_day",
    "sleep_hours",
    "phone_usage_hours",
    "social_media_hours",
    "youtube_hours",
    "gaming_hours",
    "attendance_percentage",
    "final_grade",
    "productivity_score"
]

df_typed = df

for c in integer_cols:
    df_typed = df_typed.withColumn(
        c, F.col(c).cast("integer")
    )

for c in double_cols:
    df_typed = df_typed.withColumn(
        c, F.col(c).cast("double")
    )

# Padronização da variável categórica
df_typed = df_typed.withColumn(
    "gender",
    F.initcap(F.trim(F.col("gender")))
)

print("=== TIPAGEM CONCLUÍDA ===")
df_typed.printSchema()

# COMMAND ----------

# Validação após conversão dos tipos

print("=== NULOS APÓS TIPAGEM ===")

for c in integer_cols + double_cols:
    nulos = df_typed.filter(F.col(c).isNull()).count()
    print(f"{c}: {nulos}")

# COMMAND ----------

# Verificação de valores mínimos e máximos
# para avaliar consistência e plausibilidade

numeric_cols = integer_cols + double_cols

resultado_dominios = []

for c in numeric_cols:
    stats = (
        df_typed
        .agg(
            F.min(c).alias("minimo"),
            F.max(c).alias("maximo")
        )
        .first()
    )

    resultado_dominios.append(
        (c, float(stats["minimo"]), float(stats["maximo"]))
    )

df_dominios = spark.createDataFrame(
    resultado_dominios,
    ["variavel", "minimo", "maximo"]
)

display(df_dominios)

# COMMAND ----------

# Validação específica das principais variáveis do MVP

df_typed.select(
    F.min("final_grade").alias("nota_minima"),
    F.max("final_grade").alias("nota_maxima"),
    F.min("productivity_score").alias("produtividade_minima"),
    F.max("productivity_score").alias("produtividade_maxima")
).show()

print("=== CATEGORIAS DE GÊNERO ===")
df_typed.groupBy("gender").count().orderBy("gender").show()

# COMMAND ----------

# Construção da camada Silver

df_silver = (
    df_typed
    .withColumn(
        "entertainment_hours",
        F.round(
            F.col("social_media_hours")
            + F.col("youtube_hours")
            + F.col("gaming_hours"),
            2
        )
    )
)

print("=== SILVER PREPARADA ===")
print("Registros:", df_silver.count())
print("Colunas:", len(df_silver.columns))

display(df_silver.limit(10))

# COMMAND ----------

SILVER_TABLE = "workspace.mvp_silver.student_productivity_clean"

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("Tabela Silver persistida com sucesso:")
print(SILVER_TABLE)

# COMMAND ----------

df_silver_check = spark.table(SILVER_TABLE)

print("=== VALIDAÇÃO DA SILVER ===")
print("Tabela:", SILVER_TABLE)
print("Registros:", df_silver_check.count())
print("Colunas:", len(df_silver_check.columns))

print("\nSchema:")
df_silver_check.printSchema()

display(df_silver_check.limit(10))

# COMMAND ----------

# Resumo final de qualidade dos dados
# Célula autônoma: pode ser executada independentemente das anteriores.

from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.mvp_bronze.student_productivity_raw"
QUALITY_TABLE = "workspace.mvp_silver.data_quality_summary"

# Carrega novamente a Bronze
df_quality_source = spark.table(BRONZE_TABLE)

# Considera somente os 18 atributos originais
colunas_originais = [
    c for c in df_quality_source.columns
    if not c.startswith("_")
]

total_registros = df_quality_source.count()

resultado_qualidade = []

for c in colunas_originais:

    nulos = (
        df_quality_source
        .filter(
            F.col(c).isNull() |
            (F.trim(F.col(c)) == "")
        )
        .count()
    )

    distintos = (
        df_quality_source
        .select(c)
        .distinct()
        .count()
    )

    percentual_nulos = round(
        (nulos / total_registros) * 100,
        2
    )

    resultado_qualidade.append(
        (
            c,
            total_registros,
            nulos,
            percentual_nulos,
            distintos
        )
    )

df_quality = spark.createDataFrame(
    resultado_qualidade,
    [
        "atributo",
        "total_registros",
        "valores_nulos",
        "percentual_nulos",
        "valores_distintos"
    ]
)

print("=== RESUMO DE QUALIDADE DOS DADOS ===")
print("Total de registros:", total_registros)
print("Total de atributos analisados:", len(colunas_originais))

display(df_quality)

# COMMAND ----------

# Persistência do resumo de qualidade na camada Silver

QUALITY_TABLE = "workspace.mvp_silver.data_quality_summary"

(
    df_quality.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUALITY_TABLE)
)

print("Tabela de qualidade persistida com sucesso:")
print(QUALITY_TABLE)

# COMMAND ----------

# Validação da tabela de qualidade

df_quality_check = spark.table(QUALITY_TABLE)

print("=== VALIDAÇÃO DA TABELA DE QUALIDADE ===")
print("Tabela:", QUALITY_TABLE)
print("Registros:", df_quality_check.count())
print("Colunas:", len(df_quality_check.columns))

display(df_quality_check)