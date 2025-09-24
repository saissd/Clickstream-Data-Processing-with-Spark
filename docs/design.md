# Design
- **Bronze:** typed raw tables from CSV → Parquet
- **Silver:** line_items = order_products (prior+train) ⨝ orders ⨝ products ⨝ aisles ⨝ departments
- **Gold:** department_sales, product_kpis, dow_hour_activity (+ optional MySQL sink)
- **Performance:** set shuffle partitions via config; partition writes future-ready.
