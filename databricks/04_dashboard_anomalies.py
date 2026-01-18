# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard anomalii transakcyjnych
# MAGIC
# MAGIC Notebook prezentuje wyniki detekcji anomalii zapisane w warstwie Gold.
# MAGIC Zawiera interaktywne wizualizacje umożliwiające analizę trendów anomalii
# MAGIC w czasie oraz szczegółowy przegląd wykrytych alertów.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Konfiguracja dostępu do Azure Data Lake Storage (ADLS Gen2)
# MAGIC

# COMMAND ----------

storageAccountName = "stfinanceanomalyqk8fmw"
storageAccountKey = "<SET_IN_DATABRICKS>"

spark.conf.set(
  f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net",
  storageAccountKey
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Odczyt danych alertów z warstwy Gold
# MAGIC

# COMMAND ----------

goldAlertsPath = f"abfss://gold@{storageAccountName}.dfs.core.windows.net/anomaly_alerts"

alertsDf = spark.read.format("delta").load(goldAlertsPath)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Podgląd wykrytych alertów anomalii
# MAGIC

# COMMAND ----------

display(alertsDf)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregacja liczby anomalii w czasie
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

alertsOverTime = (
    alertsDf
      .groupBy(col("window.start").alias("windowStart"))
      .agg(count("*").alias("anomalyCount"))
      .orderBy("windowStart")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Trend liczby anomalii w czasie
# MAGIC

# COMMAND ----------

display(alertsOverTime)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Użytkownicy z największą liczbą anomalii
# MAGIC

# COMMAND ----------

topUsers = (
    alertsDf
      .groupBy("userId")
      .agg(count("*").alias("anomalyCount"))
      .orderBy(desc("anomalyCount"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Top uytkownicy z anomaliami

# COMMAND ----------

display(topUsers)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Przygotowanie szczegółów alertów
# MAGIC

# COMMAND ----------

alertDetails = alertsDf.select(
    "userId",
    "avgAmount",
    "maxAmount",
    "txCount",
    col("window.start").alias("windowStart"),
    col("window.end").alias("windowEnd")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela szczegółów alertów

# COMMAND ----------

display(alertDetails)


# COMMAND ----------

# MAGIC %md
# MAGIC