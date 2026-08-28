# Databricks notebook source
# MVP Engenharia de Dados
# ETAPA 04 - Catálogo de Dados e documentação

table_comments = {
    
    "workspace.mvp_bronze.student_productivity_raw":
        "Camada Bronze. Dados brutos dos estudantes preservados conforme recebidos da fonte, acrescidos de metadados de ingestão.",

    "workspace.mvp_silver.student_productivity_clean":
        "Camada Silver. Dados dos estudantes tipados, padronizados e preparados para análise. Inclui a variável derivada entertainment_hours.",

    "workspace.mvp_gold.student_performance":
        "Camada Gold. Tabela analítica principal com granularidade de um registro por estudante.",

    "workspace.mvp_gold.correlation_with_final_grade":
        "Camada Gold. Ranking das correlações de Pearson entre variáveis numéricas e a nota final dos estudantes.",

    "workspace.mvp_gold.performance_by_gender":
        "Camada Gold. Estatísticas descritivas de desempenho, estudo, frequência, foco e entretenimento agrupadas por gênero."
}

for table, comment in table_comments.items():
    escaped_comment = comment.replace("'", "''")
    
    spark.sql(
        f"COMMENT ON TABLE {table} IS '{escaped_comment}'"
    )
    
    print("Documentada:", table)

print("\nDescrições das tabelas adicionadas com sucesso.")

# COMMAND ----------

# Documentação das colunas da tabela Gold principal

GOLD_TABLE = "workspace.mvp_gold.student_performance"

column_comments = {
    "student_id":
        "Identificador único do estudante. Domínio observado: 1 a 5999.",

    "age":
        "Idade do estudante em anos. Domínio observado: 17 a 29.",

    "gender":
        "Gênero registrado na base. Categorias observadas: Female, Male e Other.",

    "study_hours_per_day":
        "Quantidade de horas de estudo por dia. Domínio observado: 0,5 a 10 horas.",

    "sleep_hours":
        "Quantidade de horas de sono. Domínio observado: 3 a 10 horas.",

    "phone_usage_hours":
        "Tempo de uso do telefone. Domínio observado: 0,5 a 12 horas.",

    "social_media_hours":
        "Tempo dedicado a redes sociais. Domínio observado: 0 a 8 horas.",

    "youtube_hours":
        "Tempo dedicado ao YouTube. Domínio observado: 0 a 6 horas.",

    "gaming_hours":
        "Tempo dedicado a jogos. Domínio observado: 0 a 6 horas.",

    "entertainment_hours":
        "Variável derivada na Silver: soma de social_media_hours, youtube_hours e gaming_hours.",

    "breaks_per_day":
        "Quantidade de pausas realizadas por dia. Domínio observado: 1 a 14.",

    "coffee_intake_mg":
        "Consumo de cafeína registrado em miligramas. Domínio observado: 0 a 499 mg.",

    "exercise_minutes":
        "Tempo dedicado a exercícios. Domínio observado: 0 a 119 minutos.",

    "assignments_completed":
        "Quantidade de atividades concluídas. Domínio observado: 0 a 19.",

    "attendance_percentage":
        "Percentual de frequência do estudante. Domínio observado: 40,01% a 99,97%.",

    "stress_level":
        "Nível de estresse registrado na base. Domínio observado: 1 a 10.",

    "focus_score":
        "Pontuação de foco. Domínio observado: 30 a 99.",

    "productivity_score":
        "Pontuação de produtividade. Domínio observado: 1,91 a 99,9.",

    "final_grade":
        "Nota final do estudante e variável-alvo da análise. Domínio observado: 40,0 a 99,99."
}

for column, comment in column_comments.items():

    escaped_comment = comment.replace("'", "''")

    spark.sql(
        f"""
        ALTER TABLE {GOLD_TABLE}
        ALTER COLUMN `{column}`
        COMMENT '{escaped_comment}'
        """
    )

    print("Documentada:", column)

print("\nCatálogo das colunas concluído.")

# COMMAND ----------

spark.sql("""
COMMENT ON TABLE workspace.mvp_silver.data_quality_summary IS
'Resumo das métricas de qualidade dos 18 atributos originais,
incluindo completude e quantidade de valores distintos.'
""")

print("Tabela data_quality_summary documentada.")

# COMMAND ----------

# Documentação da tabela de qualidade

spark.sql("""
COMMENT ON TABLE workspace.mvp_silver.data_quality_summary IS
'Resumo das métricas de qualidade dos 18 atributos originais, incluindo completude e quantidade de valores distintos.'
""")

print("Tabela data_quality_summary documentada com sucesso.")