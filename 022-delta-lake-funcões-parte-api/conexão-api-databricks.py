# Databricks notebook source
# DBTITLE 1,Importar bibliotecas
import requests 
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Consulta resposta da API
url = 'https://brasilapi.com.br/api/feriados/v1/2024'
response = requests.get(url)
print(response)

# COMMAND ----------

# DBTITLE 1,Visualização dos dados a API
data = response.json()
print(data)

# COMMAND ----------

# DBTITLE 1,Criando DataFrame Com Spark
df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

# DBTITLE 1,Criando Database Delta
# MAGIC %sql
# MAGIC create database if not exists brasil_api

# COMMAND ----------

# DBTITLE 1,Criando View do DataFrame
df.createOrReplaceTempView("view_api_feriados")

# COMMAND ----------

# DBTITLE 1,Criando tabela delta
spark.sql("""
            create or replace table brasil_api.tb_feriados
            using delta
            as
            select * from view_api_feriados
          """)

# COMMAND ----------

# DBTITLE 1,Consulta tabela delta
# MAGIC %sql
# MAGIC select * from brasil_api.tb_feriados

# COMMAND ----------

# DBTITLE 1,Historico de versões
# MAGIC %sql
# MAGIC describe history brasil_api.tb_feriados

# COMMAND ----------

# DBTITLE 1,Detalhe da Tabela Delta
# MAGIC %sql
# MAGIC describe detail brasil_api.tb_feriados
