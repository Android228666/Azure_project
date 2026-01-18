# Databricks notebook source
# MAGIC %md
# MAGIC # Detekcja anomalii w transakcjach finansowych (warstwa Gold)
# MAGIC
# MAGIC Notebook realizuje strumieniową analizę danych z warstwy Silver w celu
# MAGIC identyfikacji potencjalnie podejrzanych transakcji finansowych.
# MAGIC Wykorzystuje okna czasowe, watermarki oraz statystyki opisowe
# MAGIC do wykrywania anomalii i zapisu alertów do warstwy Gold w Delta Lake.
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
# MAGIC ## Definicja ścieżek warstw Silver i Gold
# MAGIC

# COMMAND ----------

# bronzePath = f"abfss://bronze@{storageAccountName}.dfs.core.windows.net/transactions"
silverPath = f"abfss://silver@{storageAccountName}.dfs.core.windows.net/transactions_clean"

goldAlertsPath = f"abfss://gold@{storageAccountName}.dfs.core.windows.net/anomaly_alerts"
goldAggsPath   = f"abfss://gold@{storageAccountName}.dfs.core.windows.net/aggregates"

checkpointAlerts = f"abfss://gold@{storageAccountName}.dfs.core.windows.net/checkpoints/anomaly_alerts"
checkpointAggs   = f"abfss://gold@{storageAccountName}.dfs.core.windows.net/checkpoints/aggregates"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Odczyt strumieniowy danych z warstwy Silver
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

silverDf = spark.readStream.format("delta").load(silverPath)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregacje transakcji w oknach czasowych (per użytkownik)

# COMMAND ----------

userWindowStats = (
  silverDf
    .withWatermark("eventTime", "10 minutes")
    .groupBy(
        window(col("eventTime"), "5 minutes"),
        col("userId")
    )
    .agg(
        avg("amount").alias("avgAmount"),
        count("*").alias("txCount")
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Detekcja anomalii transakcyjnych
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

alertsDf = (
    silverDf
      .withWatermark("eventTime", "10 minutes")
      .groupBy(
          window(col("eventTime"), "5 minutes"),
          col("userId")
      )
      .agg(
          avg("amount").alias("avgAmount"),
          stddev("amount").alias("stdAmount"),
          max("amount").alias("maxAmount"),
          count("*").alias("txCount")
      )
      .withColumn(
          "isAnomaly",
          (col("maxAmount") > col("avgAmount") + col("stdAmount") * 3) |
          (col("maxAmount") > 2000)
      )
      .filter(col("isAnomaly") == True)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Zapis alertów anomalii do warstwy Gold
# MAGIC

# COMMAND ----------

alertsQuery = (
  alertsDf.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointAlerts)
    .start(goldAlertsPath)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Podgląd wykrytych anomalii w warstwie Gold
# MAGIC

# COMMAND ----------

spark.read.format("delta").load(goldAlertsPath).display()


# COMMAND ----------

for s in spark.streams.active:
    s.stop()
