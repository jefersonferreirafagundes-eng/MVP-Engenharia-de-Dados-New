# MVP de Engenharia de Dados 
# Fatores associados à nota final dos estudantes

## Visão geral

Este MVP implementa um pipeline no Databricks Free Edition com
Python/PySpark, Delta Lake e Unity Catalog. O objetivo é investigar
quais características de rotina, estudo, frequência, foco e uso de
tecnologia apresentam associação com `final_grade`.

# 1. Contexto de Negócio e Perguntas 
## Problema

Investigar se as variáveis disponíveis apresentam associação com a nota
final dos estudantes.

## Perguntas

1.  Quais variáveis numéricas apresentam maior associação linear com
    `final_grade`?
2.  Estudo, frequência, foco e atividades concluídas estão associados à
    nota?
3.  Celular, redes sociais, YouTube, jogos e entretenimento digital
    estão associados à nota?
4.  Sono e exercício estão associados à nota?
5.  Existem diferenças descritivas nas médias entre os grupos de gênero?

## Dados brutos

A execução no Databricks verificou **5.999 registros e 18 atributos
originais**: `student_id`, `age`, `gender`, `study_hours_per_day`,
`sleep_hours`, `phone_usage_hours`, `social_media_hours`,
`youtube_hours`, `gaming_hours`, `breaks_per_day`, `coffee_intake_mg`,
`exercise_minutes`, `assignments_completed`, `attendance_percentage`,
`stress_level`, `focus_score`, `final_grade` e `productivity_score`.

Arquivo: `student_productivity_distraction_dataset_20000.csv`. Apesar do
nome, a leitura efetiva retornou 5.999 registros.

## Fonte e licença

Dataset: Student Productivity & Distraction Dataset, extraído da Kaggle.
Fonte: https://raw.githubusercontent.com/jefersonferreirafagundes-eng/MVP-ML-Analytics/refs/heads/main/student_productivity_distraction_dataset_20000.csv
Carga no Databricks em: /Volumes/workspace/mvp_raw/input_files/student_productivity_distraction_dataset_20000.csv

# 2. Carga dos Dados 

A execução foi realizada no Databricks Free Edition. Foi criado o Managed Volume `/Volumes/workspace/mvp_raw/input_files`, onde o CSV
original foi carregado.

Fluxo: `CSV → Unity Catalog Volume → Bronze`

Bronze: `workspace.mvp_bronze.student_productivity_raw`.

# 3. Modelagem e Catálogo de Dados 

Foi utilizada a Arquitetura Medalhão: `Raw → Bronze → Silver → Gold`.

## Raw

`workspace.mvp_raw.input_files`

## Bronze

`workspace.mvp_bronze.student_productivity_raw` --- preservação do dado recebido e metadados de ingestão.

## Silver

-   `workspace.mvp_silver.student_productivity_clean`
-   `workspace.mvp_silver.data_quality_summary`

Responsabilidades: tipagem, padronização, qualidade e criação de `entertainment_hours`.

## Gold

-   `workspace.mvp_gold.student_performance`
-   `workspace.mvp_gold.correlation_with_final_grade`
-   `workspace.mvp_gold.performance_by_gender`

Foi adotado modelo flat por conceito, com um registro por estudante na tabela analítica principal.

## Catálogo

As tabelas e campos foram documentados no Unity Catalog com descrição,tipo e domínio observado. Exemplos:

  Campo                   Tipo     Domínio observado
  ----------------------- -------- ---------------------
  student_id              int      1--5999
  age                     int      17--29
  gender                  string   Female, Male, Other
  study_hours_per_day     double   0,5--10
  sleep_hours             double   3--10
  phone_usage_hours       double   0,5--12
  social_media_hours      double   0--8
  youtube_hours           double   0--6
  gaming_hours            double   0--6
  attendance_percentage   double   40,01--99,97
  stress_level            int      1--10
  focus_score             int      30--99
  productivity_score      double   1,91--99,9
  final_grade             double   40,0--99,99

## Linhagem

O Unity Catalog registrou `workspace.mvp_silver.student_productivity_clean → workspace.mvp_gold.student_performance`,
incluindo o notebook `03_gold_analysis` como parte do processo.

# 4. Pipeline de Dados

-   `00_setup`: criação dos schemas Raw/Bronze/Silver/Gold.
-   `01_bronze_ingestion`: leitura do CSV, validação inicial, metadados
    e persistência Delta.
-   `02_silver_quality`: duplicidade, nulos, tipagem, padronização,
    domínios, `entertainment_hours`, Silver e resumo de qualidade.
-   `03_gold_analysis`: tabela analítica, correlações e análise por
    gênero.
-   `04_catalog_lineage`: documentação no Unity Catalog.

# 5. Qualidade de Dados 

Foram analisados os 18 atributos originais.

## Completude

Todos os 18 atributos apresentaram **0 valores nulos (0%)** nos 5.999 registros. Não foi necessária imputação.

## Unicidade

-   Registros: 5.999
-   `student_id` distintos: 5.999
-   Duplicidades de `student_id`: 0

## Consistência

Foram examinados mínimos e máximos. Nas verificações realizadas, não foram identificados valores fora dos limites estruturais avaliados que
justificassem exclusão automática.

## Variável derivada

`entertainment_hours = social_media_hours + youtube_hours + gaming_hours`.

# 6. Análise de Dados 

## Pergunta 1 --- maior associação com a nota

    Posição Variável                  Correlação
  --------- ----------------------- ------------
          1 youtube_hours                -0,0379
          2 gaming_hours                 +0,0332
          3 stress_level                 -0,0288
          4 study_hours_per_day          -0,0263
          5 sleep_hours                  +0,0217
          6 attendance_percentage        -0,0188
          7 assignments_completed        +0,0154
          8 productivity_score           -0,0126
          9 focus_score                  -0,0098
         10 age                          +0,0094

A maior correlação absoluta foi 0,0379. Portanto, nenhuma variável analisada apresentou associação linear relevante com `final_grade`.

## Pergunta 2 --- estudo, frequência, foco e atividades

-   estudo: -0,0263
-   frequência: -0,0188
-   foco: -0,0098
-   atividades: +0,0154

As associações são muito próximas de zero.

## Pergunta 3 --- tecnologia e entretenimento

-   celular: -0,0032
-   redes sociais: +0,0016
-   YouTube: -0,0379
-   games: +0,0332
-   entretenimento digital: -0,0011

Não foi observada associação linear relevante.

## Pergunta 4 --- sono e exercício

-   sono: +0,0217
-   exercício: +0,0022

As correlações são muito próximas de zero.

## Pergunta 5 --- gênero

  --------------------------------------------------------------------------
  Gênero      Estudantes   Nota média Estudo médio   Frequência   Foco médio
                                                          média 
  --------- ------------ ------------ ------------ ------------ ------------
  Female           2.868        70,21         5,20        69,90        63,94

  Male             2.897        70,25         5,20        69,29        64,28

  Other              234        69,71         5,03        68,79        65,30


A diferença entre a maior e a menor média foi 0,54 ponto. A análise é descritiva e não permite atribuir causalidade ao gênero.

## Discussão geral

Nos 5.999 registros, as variáveis disponíveis apresentam correlações lineares muito baixas com a nota final. O resultado indica que, neste
dataset, as variáveis analisadas isoladamente não apresentam associação linear relevante com o desempenho final.

# 7. Autoavaliação

# Objetivos atingidos

o MVP implementou um pipeline de engenharia de dados em ambiente de nuvem no Databricks Free Edition, estruturado segundo a Arquitetura Medalhão. Foram criadas e persistidas tabelas Delta nas camadas Bronze, Silver e Gold, realizadas verificações de qualidade, documentados ativos no Unity Catalog, registrada a linhagem Silver → Gold e produzidas análises diretamente relacionadas às perguntas definidas no contexto de negócio. Dessa forma, o trabalho não se limitou ao cálculo de correlações: ele demonstrou o ciclo de ingestão, tratamento, governança, rastreabilidade e disponibilização analítica dos dados.

# Dificuldades

A implementação exigiu ajustes específicos do Databricks e do Unity Catalog, principalmente na organização dos metadados de origem e na estruturação dos ativos entre volumes, schemas e tabelas. Outro ponto relevante foi a quantidade efetivamente lida: embora o nome do arquivo contenha a referência a 20.000 registros, a execução utilizada no MVP retornou 5.999. Por esse motivo, todas as métricas de qualidade e análises deste relatório foram apresentadas com base nos 5.999 registros efetivamente processados, evitando assumir uma quantidade não confirmada pela execução.

# Limitações

A análise é observacional e baseada predominantemente em correlações lineares. Correlação não demonstra causalidade, e os coeficientes encontrados foram muito baixos. Além disso, os resultados dependem das variáveis existentes no dataset e da forma como elas foram coletadas e representadas. O MVP não testa mecanismos causais, não acompanha os mesmos estudantes longitudinalmente e não permite concluir que alterações em hábitos específicos produziriam mudanças na nota final. Consequentemente, as conclusões devem ser interpretadas como evidências descritivas e associativas restritas a este conjunto de dados.

# Trabalhos futuros

Uma evolução do projeto pode incorporar novas fontes educacionais, dados longitudinais e variáveis adicionais devidamente anonimizadas, permitindo analisar dimensões não representadas no conjunto atual. Também podem ser investigadas relações não lineares e interações entre variáveis, além da inclusão de testes automatizados de qualidade e de um pipeline incremental para cenários com atualização contínua. Modelos preditivos podem ser acrescentados em uma etapa posterior, desde que se mantenha explícita a distinção entre capacidade de predição, associação estatística e evidência causal. Essa evolução permitiria avaliar se combinações de atributos oferecem informação que não aparece nas correlações individuais observadas neste MVP.
