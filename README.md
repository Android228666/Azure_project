# Wykrywanie anomalii w transakcjach finansowych (Azure, Streaming)

## Opis projektu
Projekt realizuje end-to-end strumieniowy potok danych służący do wykrywania anomalii
w transakcjach finansowych. Symulowane dane transakcyjne są przesyłane do Azure Event Hubs,
następnie przetwarzane w czasie rzeczywistym w Azure Databricks (Spark Structured Streaming).
Dane przechodzą przez kolejne warstwy Bronze, Silver i Gold w Azure Data Lake Storage (Delta Lake),
a wyniki analizy prezentowane są w interaktywnym dashboardzie.

Projekt został zrealizowany w architekturze Kappa (streaming-first).

## Architektura
Event Hubs -> Databricks Structured Streaming -> Delta Lake (Bronze / Silver / Gold) -> Dashboard

## Technologie
- Azure Event Hubs
- Azure Databricks (Spark Structured Streaming)
- Azure Data Lake Storage Gen2
- Delta Lake
- Terraform (IaC)
- GitHub Actions (CI/CD)
- Python / PySpark
- Power BI / Databricks Dashboards

## Struktura repozytorium
databricks/  - notebooki Databricks (.py)  
iac/         - infrastruktura jako kod (Terraform)  
.github/     - pipeline CI/CD  
README.md    - dokumentacja projektu  

## Etapy przetwarzania
1. Bronze – ingest strumieniowy danych z Event Hubs  
2. Silver – czyszczenie, walidacja, deduplikacja (watermark)  
3. Gold – detekcja anomalii w oknach czasowych  
4. Dashboard – wizualizacja trendów i alertów  

## Uruchomienie
1. Terraform: terraform init && terraform apply  
2. Databricks: uruchomić notebooki w kolejności 01 -> 04  

## Bezpieczeństwo
Sekrety i klucze nie są przechowywane w repozytorium.
Konfiguracja odbywa się bezpośrednio w Databricks.

## Cleanup
Po zakończeniu projektu:
terraform destroy

## Autor
Andrii Khomenko
