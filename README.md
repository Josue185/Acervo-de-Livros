# Acervo-de-Livros
Neste projeto, utilizamos o PySpark, uma interface Python para Apache Spark, para realizar a limpeza, padronização e análise de um dataset contendo informações sobre procedimentos realizados em um acervo de obras raras. O dataset original é composto pelas colunas "Localização", "Setor", e "Mês e ano procedimento", e apresenta desafios comuns de tratamento de dados, incluindo a padronização de formatos de datas e a remoção de duplicatas.

Etapas de Processamento:
Início da Sessão Spark: Utilizamos a SparkSession para iniciar uma sessão Spark, criando o ponto de entrada para nossa aplicação PySpark. Isso nos permite acessar os recursos do Spark, incluindo o processamento distribuído e otimizações de execução.

Carregamento do Dataset: O dataset é carregado a partir de um arquivo CSV, utilizando o método spark.read.csv. A leitura é configurada para tratar a primeira linha como cabeçalho (header=True) e inferir o esquema dos dados (inferSchema=True), permitindo que o Spark identifique automaticamente os tipos de dados das colunas.

![Screenshot 2024-04-11 13 26 48](https://github.com/Josue185/Acervo-de-Livros/assets/92592495/107ffdb7-0f79-433c-be04-7be66979c279)


Exploração Inicial: Realizamos uma exploração inicial do dataset com métodos como show, printSchema, e count, visando compreender a estrutura dos dados, os tipos de dados das colunas e o volume de registros.

Limpeza de Dados:

Remoção de Duplicatas: Utilizamos o método dropDuplicates para eliminar registros duplicados, garantindo a unicidade dos dados para análises subsequentes.
Tratamento de Valores Nulos: Aplicamos o método na.drop para remover linhas que contêm valores nulos, melhorando a qualidade do dataset para análise.
Padronização de Datas: Enfrentamos o desafio de padronizar os formatos variados na coluna "Mês e ano procedimento". Para isso, desenvolvemos uma função UDF (padronizar_data) que converte os formatos de data para um padrão uniforme "MM-YYYY". A função UDF trata diferentes formatos encontrados, incluindo a conversão de nomes abreviados de meses para seus respectivos números e a padronização do formato de ano.

![Screenshot 2024-04-11 13 27 18](https://github.com/Josue185/Acervo-de-Livros/assets/92592495/a86104b0-092a-4a4d-8578-3d6dce3c8e67)
![Screenshot 2024-04-11 13 27 26](https://github.com/Josue185/Acervo-de-Livros/assets/92592495/156cfd53-2ac7-4848-9511-411c220a5f09)
![Screenshot 2024-04-11 13 27 34](https://github.com/Josue185/Acervo-de-Livros/assets/92592495/0e406ee2-0f29-4ff0-86f3-c85e1e19c76d)
![Screenshot 2024-04-11 13 27 40](https://github.com/Josue185/Acervo-de-Livros/assets/92592495/09118a48-a02f-47d0-8cf2-f8fcf76cf2c5)


Agrupamento e Análise:

Agrupamento por Ano: Com os dados de data padronizados, agrupamos o dataset por ano, utilizando o método groupBy.
Exportação dos Dados Processados: Por fim, salvamos o dataset processado e analisado em um novo arquivo CSV, utilizando o método write.csv, permitindo a utilização desses dados para análises futuras ou compartilhamento.

Este processo exemplifica a aplicação de técnicas de limpeza e análise de dados em larga escala utilizando PySpark, destacando sua capacidade de processar e analisar datasets complexos de maneira eficiente e distribuída.
