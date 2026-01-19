# Databricks notebook source
# MAGIC %md
# MAGIC # Przetwarzanie wsadowe – dzienne statystyki anomalii (warstwa Gold) 
# MAGIC Ten notebook realizuje wsadowe (batch) przetwarzanie danych na podstawie wyników detekcji anomalii z warstwy Gold.
# MAGIC Celem jest obliczenie dziennych statystyk anomalii (liczba alertów, liczba użytkowników z anomaliami, średnia i maksymalna wartość anomalii) oraz zapis wyników do osobnej tabeli Delta (daily_stats), przeznaczonej do raportowania i dashboardów.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Konfiguracja połączenia z Azure Data Lake Storage

# COMMAND ----------

storageAccountName = "stfinanceanomalyqk8fmw"
storageAccountKey = "NvbKS3k8GoGa+VyRNS4yl47HilP83YIMEDE0Cm137CkxGnLih/b+7Qm71me1+MeHauT+CwzvyfJ2+AStuQP/vw=="

spark.conf.set(
  f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net",
  storageAccountKey
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definicja ścieżek danych (Gold)

# COMMAND ----------

goldAlertsPath = f"abfss://gold@{storageAccountName}.dfs.core.windows.net/anomaly_alerts"
dailyStatsPath = f"abfss://gold@{storageAccountName}.dfs.core.windows.net/daily_stats"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Odczyt alertów anomalii z warstwy Gold

# COMMAND ----------

alertsDf = spark.read.format("delta").load(goldAlertsPath)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregacja dzienna anomalii (batch processing)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, count, countDistinct, avg, max

dailyStatsDf = (
    alertsDf
    .withColumn("eventDate", to_date(col("window.start")))
    .groupBy("eventDate")
    .agg(
        count("*").alias("anomalyCount"),
        countDistinct("userId").alias("usersWithAnomalies"),
        avg("maxAmount").alias("avgAnomalyAmount"),
        max("maxAmount").alias("maxAnomalyAmount")
    )
    .orderBy("eventDate")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Zapis dziennych statystyk do warstwy Gold

# COMMAND ----------

dailyStatsDf.write \
    .format("delta") \
    .mode("overwrite") \
    .save(dailyStatsPath)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Podgląd dziennych statystyk anomalii

# COMMAND ----------

spark.read.format("delta").load(dailyStatsPath).display()
