# Wykrywanie anomalii w transakcjach finansowych (Azure, Streaming)

## Opis projektu
Projekt realizuje kompletny, end-to-end strumieniowy potok danych służący do wykrywania anomalii w transakcjach finansowych.  
Symulowane dane transakcyjne są przesyłane do **Azure Event Hubs**, a następnie przetwarzane w czasie rzeczywistym w **Azure Databricks** z wykorzystaniem **Spark Structured Streaming**.

Dane przechodzą przez kolejne warstwy **Bronze**, **Silver** oraz **Gold** w **Azure Data Lake Storage Gen2** z użyciem **Delta Lake**.  
Wyniki analizy, w tym wykryte anomalie, prezentowane są w postaci interaktywnych dashboardów.

Projekt został zrealizowany w architekturze **Kappa (streaming-first)**.

---

## Architektura
```
Event Hubs
    ↓
Databricks Structured Streaming
    ↓
Delta Lake (Bronze / Silver / Gold)
    ↓
Dashboard
```

---

## Technologie
- Azure Event Hubs  
- Azure Databricks (Spark Structured Streaming)  
- Azure Data Lake Storage Gen2  
- Delta Lake  
- Terraform (Infrastructure as Code)  
- GitHub Actions (CI/CD)  
- Python / PySpark  
- Power BI / Databricks Dashboards  

---

## Struktura repozytorium
```
databricks/   - notebooki Databricks (.py)
iac/          - infrastruktura jako kod (Terraform)
.github/      - pipeline CI/CD (GitHub Actions)
README.md     - dokumentacja projektu
```

---

## Etapy przetwarzania danych
- **Bronze** – strumieniowy ingest danych z Azure Event Hubs  
- **Silver** – czyszczenie danych, walidacja schematu oraz deduplikacja (watermark)  
- **Gold** – detekcja anomalii w oknach czasowych oraz agregacje analityczne  
- **Dashboard** – wizualizacja trendów oraz alertów anomalii  

---

## Uruchomienie projektu

### Infrastruktura (Terraform)
```bash
terraform init
terraform apply
```

### Przetwarzanie danych (Databricks)
Notebooki należy uruchomić w kolejności:
```
01_ingest_bronze
02_transform_silver
03_detect_anomalies_gold
04_dashboard_anomalies
05_batch_daily_stats
```

---

## Bezpieczeństwo
- W projekcie nie są wykorzystywane dane wrażliwe.
- Sekrety oraz klucze dostępu nie są przechowywane w repozytorium Git.
- Konfiguracja połączeń realizowana jest bezpośrednio w środowisku Azure Databricks.

---

## Cleanup
Po zakończeniu projektu wszystkie zasoby mogą zostać usunięte za pomocą:
```bash
terraform destroy
```
lub poprzez usunięcie całej Resource Group w Azure Portal.

---

## Autor
**Andrii Khomenko**
