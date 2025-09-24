#!/usr/bin/env bash
set -euo pipefail
# Requires mysql JDBC driver on classpath; easiest via --packages when using spark-submit on a cluster.
python spark_pyspark/etl_instacart.py
