# Databricks notebook source
# MVP Engenharia de Dados
# ETAPA 03 - Camada Gold e análise das variáveis associadas à nota final

from pyspark.sql import functions as F

SILVER_TABLE = "workspace.mvp_silver.student_productivity_clean"
GOLD_TABLE = "workspace.mvp_gold.student_performance"

df = spark.table(SILVER_TABLE)

print("=== SILVER CARREGADA ===")
print("Registros:", df.count())
print("Colunas:", len(df.columns))

# COMMAND ----------

# Seleção das variáveis relevantes para a análise

analysis_cols = [
    "student_id",
    "age",
    "gender",
    "study_hours_per_day",
    "sleep_hours",
    "phone_usage_hours",
    "social_media_hours",
    "youtube_hours",
    "gaming_hours",
    "entertainment_hours",
    "breaks_per_day",
    "coffee_intake_mg",
    "exercise_minutes",
    "assignments_completed",
    "attendance_percentage",
    "stress_level",
    "focus_score",
    "productivity_score",
    "final_grade"
]

df_gold = df.select(*analysis_cols)

print("=== GOLD PREPARADA ===")
print("Registros:", df_gold.count())
print("Colunas:", len(df_gold.columns))

display(df_gold.limit(10))

# COMMAND ----------

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

print("Tabela Gold persistida com sucesso:")
print(GOLD_TABLE)

# COMMAND ----------

# Correlação das variáveis numéricas com a nota final

variaveis = [
    "age",
    "study_hours_per_day",
    "sleep_hours",
    "phone_usage_hours",
    "social_media_hours",
    "youtube_hours",
    "gaming_hours",
    "entertainment_hours",
    "breaks_per_day",
    "coffee_intake_mg",
    "exercise_minutes",
    "assignments_completed",
    "attendance_percentage",
    "stress_level",
    "focus_score",
    "productivity_score"
]

resultados = []

for variavel in variaveis:
    correlacao = df_gold.stat.corr(
        variavel,
        "final_grade"
    )

    resultados.append(
        (
            variavel,
            float(correlacao),
            abs(float(correlacao))
        )
    )

df_correlacoes = spark.createDataFrame(
    resultados,
    [
        "variavel",
        "correlacao_final_grade",
        "correlacao_absoluta"
    ]
)

df_correlacoes = df_correlacoes.orderBy(
    F.desc("correlacao_absoluta")
)

display(df_correlacoes)

# COMMAND ----------

# Ranking final das associações com final_grade
# Valores arredondados para facilitar interpretação e documentação

df_ranking = (
    df_correlacoes
    .select(
        "variavel",
        F.round("correlacao_final_grade", 4).alias("correlacao"),
        F.round("correlacao_absoluta", 4).alias("forca_associacao")
    )
    .orderBy(F.desc("forca_associacao"))
)

display(df_ranking)

# COMMAND ----------

# Persistência do ranking de correlações na camada Gold

CORRELATION_TABLE = (
    "workspace.mvp_gold.correlation_with_final_grade"
)

(
    df_ranking.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CORRELATION_TABLE)
)

print("Tabela de correlações persistida com sucesso:")
print(CORRELATION_TABLE)

# COMMAND ----------

# Resumo das associações com a nota final

df_resumo = spark.createDataFrame(
    [
        ("Estudo", "study_hours_per_day",
         df_gold.stat.corr("study_hours_per_day", "final_grade")),

        ("Frequência", "attendance_percentage",
         df_gold.stat.corr("attendance_percentage", "final_grade")),

        ("Foco", "focus_score",
         df_gold.stat.corr("focus_score", "final_grade")),

        ("Atividades", "assignments_completed",
         df_gold.stat.corr("assignments_completed", "final_grade")),

        ("Celular", "phone_usage_hours",
         df_gold.stat.corr("phone_usage_hours", "final_grade")),

        ("Redes sociais", "social_media_hours",
         df_gold.stat.corr("social_media_hours", "final_grade")),

        ("YouTube", "youtube_hours",
         df_gold.stat.corr("youtube_hours", "final_grade")),

        ("Games", "gaming_hours",
         df_gold.stat.corr("gaming_hours", "final_grade")),

        ("Entretenimento digital", "entertainment_hours",
         df_gold.stat.corr("entertainment_hours", "final_grade")),

        ("Sono", "sleep_hours",
         df_gold.stat.corr("sleep_hours", "final_grade")),

        ("Exercício", "exercise_minutes",
         df_gold.stat.corr("exercise_minutes", "final_grade"))
    ],
    ["dimensao", "variavel", "correlacao"]
)

df_resumo = (
    df_resumo
    .withColumn(
        "correlacao",
        F.round("correlacao", 4)
    )
)

display(df_resumo)

# COMMAND ----------

# Análise descritiva por gênero

df_genero = (
    df_gold
    .groupBy("gender")
    .agg(
        F.count("*").alias("estudantes"),
        F.round(F.avg("final_grade"), 2).alias("nota_media"),
        F.round(F.avg("study_hours_per_day"), 2).alias("horas_estudo_media"),
        F.round(F.avg("attendance_percentage"), 2).alias("frequencia_media"),
        F.round(F.avg("focus_score"), 2).alias("foco_medio"),
        F.round(F.avg("entertainment_hours"), 2).alias("entretenimento_medio")
    )
    .orderBy("gender")
)

display(df_genero)

# COMMAND ----------

GENDER_TABLE = "workspace.mvp_gold.performance_by_gender"

(
    df_genero.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GENDER_TABLE)
)

print("Tabela persistida:")
print(GENDER_TABLE)