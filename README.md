# LakeHouse_Architecture
A local Data Lakehouse prototype implementing the Medallion Architecture (Bronze, Silver, and Gold layers) using Docker, Apache Spark (PySpark), MinIO Object Storage, and Streamlit. Built as a collaborative university capstone technical report to demonstrate automated end-to-end ETL processing, data quality enforcement, and business reporting.

### Key Features of Our Implementation:
* **Unified One-Click Pipeline Execution:** Features a single integrated button in Streamlit that triggers a chained, automated end-to-end PySpark ETL pipeline (Bronze ➔ Silver ➔ Gold).
* **Storage Layer (MinIO Object Storage):** Decoupled storage utilizing S3-compatible local buckets (`bronze` for raw CSVs, `silver` for standardized Parquet, and `gold` for business-aggregated Parquet).
* **Compute Layer (Apache Spark / PySpark):** In-memory distributed computation running schema standardization, lowecasing column headers, whitespace trimming, row deduplication, null-value handling, and strict mathematical rounding (`round(..., 2)`) for business summary tables.
* **Web Presentation Layer (Streamlit & Plotly/Matplotlib):** Implements dynamic visual reporting via KPI cards and executive data frames, complete with a critical frontend optimization constraint (`.limit(100)`) to maintain responsive UI rendering against large datasets.
