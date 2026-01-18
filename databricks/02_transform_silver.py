# Databricks notebook source
# MAGIC %md
# MAGIC # Transformacja danych do warstwy Silver (Streaming)
# MAGIC
# MAGIC Notebook odpowiada za strumieniowe przetwarzanie danych z warstwy Bronze.
# MAGIC W tym etapie dane są czyszczone, walidowane, deduplikowane oraz przygotowane
# MAGIC do dalszej analizy. Przetworzone dane zapisywane są do warstwy Silver
# MAGIC w formacie Delta Lake z wykorzystaniem watermarków i checkpointingu.
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
# MAGIC ## Definicja ścieżek danych dla warstw Bronze i Silver
# MAGIC

# COMMAND ----------

bronzePath = f"abfss://bronze@{storageAccountName}.dfs.core.windows.net/transactions"
silverPath = f"abfss://silver@{storageAccountName}.dfs.core.windows.net/transactions_clean"
checkpointPath = f"abfss://silver@{storageAccountName}.dfs.core.windows.net/checkpoints/transactions_clean"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Odczyt strumieniowy danych z warstwy Bronze
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

bronzeDf = spark.readStream.format("delta").load(bronzePath)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Czyszczenie i walidacja danych transakcyjnych
# MAGIC

# COMMAND ----------

cleanDf = (
  bronzeDf
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("currency", upper(col("currency")))
    .filter(col("transactionId").isNotNull())
    .filter(col("eventTime").isNotNull())
    .filter(col("amount").isNotNull())
    .filter(col("amount") > 0)
    .filter(col("amount") < 10000)
    .filter(col("currency") == "CHF")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplikacja danych z wykorzystaniem watermarków
# MAGIC

# COMMAND ----------

dedupDf = (
  cleanDf
    .withWatermark("eventTime", "10 minutes")
    .dropDuplicates(["transactionId"])
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Zapis przetworzonych danych do warstwy Silver
# MAGIC

# COMMAND ----------

query = (
  dedupDf.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointPath)
    .start(silverPath)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Podgląd danych zapisanych w warstwie Silver
# MAGIC

# COMMAND ----------

spark.read.format("delta").load(silverPath).display()


# COMMAND ----------

# for s in spark.streams.active:
#     s.stop()
 