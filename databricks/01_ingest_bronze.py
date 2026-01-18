# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest transakcji finansowych do warstwy Bronze (Streaming)

# COMMAND ----------

# MAGIC %md
# MAGIC Notebook odpowiada za strumieniowy ingest danych transakcyjnych z Azure Event Hubs,
# MAGIC parsowanie wiadomości JSON oraz zapis surowych danych do warstwy Bronze w Azure Data Lake Storage
# MAGIC w formacie Delta Lake wraz z checkpointingiem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Konfiguracja źródła danych – Azure Event Hubs

# COMMAND ----------

import json
from datetime import datetime, timezone, timedelta

eventHubConnStr = "<SET_IN_DATABRICKS>"
encryptedConnStr = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnStr)

start_time = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

ehConf = {
  "eventhubs.connectionString": encryptedConnStr,
  "eventhubs.entityPath": "transactionsStream",
  "eventhubs.consumerGroup": "$Default",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Odczyt strumieniowy zdarzeń z Event Hubs
# MAGIC

# COMMAND ----------

raw = (
  spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

q = (
  raw.selectExpr("CAST(body AS STRING) AS json")
     .writeStream
     .format("memory")
     .queryName("eh_debug")
     .outputMode("append")
     .start()
)

q.awaitTermination(10)
print("exception:", q.exception())


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
# MAGIC ## Weryfikacja struktury kontenera Bronze
# MAGIC

# COMMAND ----------

dbutils.fs.ls(f"abfss://bronze@{storageAccountName}.dfs.core.windows.net/")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Definicja schematu danych transakcyjnych
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from pyspark.sql.functions import from_json, col, to_timestamp

transactionSchema = StructType([
    StructField("transactionId", StringType()),
    StructField("userId", StringType()),
    StructField("merchantId", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
    StructField("channel", StringType()),
    StructField("timestamp", StringType()),
    StructField("deviceId", StringType()),
    StructField("isInjectedAnomaly", BooleanType())
])

parsedDf = (
  raw.selectExpr("CAST(body AS STRING) as json")
     .select(from_json(col("json"), transactionSchema).alias("data"))
     .select("data.*")
     .withColumn("eventTime", to_timestamp("timestamp"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zapis danych do warstwy Bronze (Delta Lake)
# MAGIC

# COMMAND ----------

bronzePath = f"abfss://bronze@{storageAccountName}.dfs.core.windows.net/transactions"
checkpointPath = f"abfss://bronze@{storageAccountName}.dfs.core.windows.net/checkpoints/transactions"

query = (
  parsedDf.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointPath)
    .start(bronzePath)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Podgląd danych zapisanych w warstwie Bronze
# MAGIC

# COMMAND ----------

spark.read.format("delta").load(bronzePath).display()


# COMMAND ----------

# for s in spark.streams.active:
#     s.stop()


# COMMAND ----------

