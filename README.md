# Clickstream-Data-Processing-with-Spark (Instacart)

End-to-end **Spark** pipeline built to match the Wiland, Inc. Data Engineer JD.
It processes the **Instacart Online Grocery Basket** dataset (3M+ orders) into **Bronze → Silver → Gold** layers
and writes business KPIs ready for analytics. The repo demonstrates:


- **Technologies:** Spark (PySpark + Scala), HDFS-ready I/O, Linux scripts, Python, optional MySQL sink
- **Lifecycle:** config-driven jobs, unit tests (PyTest), docs, and release/run scripts
- **Collaboration:** product requirements → technical design mapping (`docs/`)

## 📂 Dataset

This project uses the **Instacart Online Grocery Basket Analysis** dataset (3M+ orders, 200k users).  
Due to GitHub file size limits, the raw CSV files are **not included** in this repository.  

👉 Download the dataset from Kaggle:  
[Instacart Online Grocery Basket Dataset]([https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-datase](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset)t)  

After downloading, place the following CSV files in the `data/` folder:  
- `orders.csv`  
- `order_products__prior.csv`  
- `order_products__train.csv`  
- `products.csv`  
- `aisles.csv`  
- `departments.csv`  

For quick testing, a small demo file `sample_clickstream.csv` is included in the repo.  

## Architecture
```
CSV (orders, order_products, products, aisles, departments)
      └─ Bronze (typed raw parquet)
            └─ Silver (joined line_items with product/aisle/department context)
                  └─ Gold (department_sales, product_kpis, dow_hour_activity)
                                   └─ Optional: JDBC sink (MySQL)
```

## Quickstart
```bash
# 1) Python deps
pip install -r requirements.txt

# 2) Configure paths
#   Put Instacart CSVs under ./data/ and verify paths in conf/config.yaml

# 3) Run PySpark ETL (local)
bash scripts/run_pyspark_instacart.sh

# 4) Run unit tests
pytest -q

# 5) (Optional) Start MySQL and write gold tables
docker compose up -d
bash scripts/run_pyspark_instacart_with_jdbc.sh
```

## Outputs
- `data/bronze/*` (orders, order_products_prior/train, products, aisles, departments)  
- `data/silver/line_items`  
- `data/gold/department_sales`, `data/gold/product_kpis`, `data/gold/dow_hour_activity`  

## Mapping to Wiland JD
- **Spark / PySpark / Scala / HDFS:** Dual implementations, HDFS paths supported via config
- **Linux:** Bash run scripts for spark-submit
- **RDBMS (MySQL):** JDBC sink + docker-compose for local DB
- **QA / Release:** Unit tests (PyTest) for core transforms; config-driven, reproducible jobs
- **Cross-functional:** `docs/requirements.md` & `docs/design.md` translate business KPIs to technical plan
