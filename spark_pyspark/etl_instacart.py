#!/usr/bin/env python3
import os, yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_cfg(path="conf/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)

def spark_session(app_name, shuffle_parts):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", str(shuffle_parts))
            .getOrCreate())

def read_csv(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)

def write(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)

def bronze(spark, app):
    orders = read_csv(spark, app["orders"]) \
        .withColumn("order_dow", F.col("order_dow").cast("int")) \
        .withColumn("order_hour_of_day", F.col("order_hour_of_day").cast("int")) \
        .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("double"))

    def typed_op(path):
        return read_csv(spark, path) \
            .withColumn("order_id", F.col("order_id").cast("int")) \
            .withColumn("product_id", F.col("product_id").cast("int")) \
            .withColumn("add_to_cart_order", F.col("add_to_cart_order").cast("int")) \
            .withColumn("reordered", F.col("reordered").cast("int"))

    op_prior = typed_op(app["order_products_prior"])
    op_train = typed_op(app["order_products_train"])

    products = read_csv(spark, app["products"]) \
        .withColumn("product_id", F.col("product_id").cast("int")) \
        .withColumn("aisle_id", F.col("aisle_id").cast("int")) \
        .withColumn("department_id", F.col("department_id").cast("int"))

    aisles = read_csv(spark, app["aisles"]).withColumn("aisle_id", F.col("aisle_id").cast("int"))
    depts  = read_csv(spark, app["departments"]).withColumn("department_id", F.col("department_id").cast("int"))

    root = app["bronze_root"]
    write(orders,   os.path.join(root, "orders"))
    write(op_prior, os.path.join(root, "order_products_prior"))
    write(op_train, os.path.join(root, "order_products_train"))
    write(products, os.path.join(root, "products"))
    write(aisles,   os.path.join(root, "aisles"))
    write(depts,    os.path.join(root, "departments"))

    return dict(orders=orders, op_prior=op_prior, op_train=op_train,
                products=products, aisles=aisles, depts=depts)

def silver(t, app):
    op_all = t["op_prior"].unionByName(t["op_train"])
    li = (op_all
        .join(t["orders"],   "order_id", "left")
        .join(t["products"], "product_id", "left")
        .join(t["aisles"],   "aisle_id", "left")
        .join(t["depts"],    "department_id", "left")
        .withColumn("is_reorder", F.col("reordered").cast("int"))
        .withColumn("is_first_purchase", (F.col("reordered")==0).cast("int"))
    )
    write(li, os.path.join(app["silver_root"], "line_items"))
    return {"line_items": li}

def gold(s, app, jdbc=None):
    li = s["line_items"]

    dept = (li.groupBy("department_id","department")
              .agg(
                F.count("*").alias("total_items"),
                F.countDistinct("user_id").alias("distinct_users"),
                F.avg("is_reorder").alias("reorder_rate")
              )
              .orderBy(F.desc("total_items")))
    write(dept, os.path.join(app["gold_root"], "department_sales"))

    prod = (li.groupBy("product_id","product_name","aisle","department")
              .agg(
                F.count("*").alias("orders"),
                F.sum("is_reorder").alias("reorders"),
                F.countDistinct("user_id").alias("distinct_users")
              )
              .withColumn("reorder_rate", (F.col("reorders")/F.col("orders")))
              .orderBy(F.desc("orders")))
    write(prod, os.path.join(app["gold_root"], "product_kpis"))

    dowhr = (li.groupBy("order_dow","order_hour_of_day")
               .agg(
                 F.countDistinct("order_id").alias("total_orders"),
                 F.countDistinct("user_id").alias("unique_users"),
                 F.count("*").alias("total_items")
               )
               .orderBy("order_dow","order_hour_of_day"))
    write(dowhr, os.path.join(app["gold_root"], "dow_hour_activity"))

    # Optional JDBC sink
    if jdbc:
        for name, df in [("department_sales", dept), ("product_kpis", prod), ("dow_hour_activity", dowhr)]:
            (df.write
               .format("jdbc")
               .option("url", jdbc["url"])
               .option("user", jdbc["user"])
               .option("password", jdbc["password"])
               .option("dbtable", jdbc["table_prefix"] + name)
               .option("driver", jdbc["driver"])
               .mode(jdbc.get("mode","append"))
               .save())

def main():
    cfg = load_cfg()
    app = cfg["app"]
    spark = spark_session("Wiland-Instacart-ETL", app.get("shuffle_partitions", 200))

    bronze_tbls = bronze(spark, app)
    silver_tbls = silver(bronze_tbls, app)
    gold(silver_tbls, app, jdbc=cfg.get("jdbc"))

    print("✅ ETL complete. Bronze/Silver/Gold written under:", app["bronze_root"], app["silver_root"], app["gold_root"])

if __name__ == "__main__":
    main()
