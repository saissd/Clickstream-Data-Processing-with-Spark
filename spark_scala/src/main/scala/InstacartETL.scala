package wiland

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object InstacartETL {
  def bronze(spark: SparkSession, basePath: String): Map[String, DataFrame] = {
    val orders   = spark.read.option("header", "true").option("inferSchema","true").csv(s"$basePath/orders.csv")
      .withColumn("order_dow", col("order_dow").cast("int"))
      .withColumn("order_hour_of_day", col("order_hour_of_day").cast("int"))
      .withColumn("days_since_prior_order", col("days_since_prior_order").cast("double"))

    def typedOP(path: String): DataFrame = {
      spark.read.option("header","true").option("inferSchema","true").csv(path)
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("product_id", col("product_id").cast("int"))
        .withColumn("add_to_cart_order", col("add_to_cart_order").cast("int"))
        .withColumn("reordered", col("reordered").cast("int"))
    }

    val opPrior = typedOP(s"$basePath/order_products__prior.csv")
    val opTrain = typedOP(s"$basePath/order_products__train.csv")

    val products = spark.read.option("header","true").option("inferSchema","true").csv(s"$basePath/products.csv")
      .withColumn("product_id", col("product_id").cast("int"))
      .withColumn("aisle_id", col("aisle_id").cast("int"))
      .withColumn("department_id", col("department_id").cast("int"))

    val aisles = spark.read.option("header","true").option("inferSchema","true").csv(s"$basePath/aisles.csv")
      .withColumn("aisle_id", col("aisle_id").cast("int"))
    val depts  = spark.read.option("header","true").option("inferSchema","true").csv(s"$basePath/departments.csv")
      .withColumn("department_id", col("department_id").cast("int"))

    Map("orders"->orders, "opPrior"->opPrior, "opTrain"->opTrain,
        "products"->products, "aisles"->aisles, "depts"->depts)
  }

  def silver(t: Map[String, DataFrame]): DataFrame = {
    val opAll = t("opPrior").unionByName(t("opTrain"))
    opAll
      .join(t("orders"), "order_id")
      .join(t("products"), "product_id")
      .join(t("aisles"), "aisle_id")
      .join(t("depts"), "department_id")
      .withColumn("is_reorder", col("reordered").cast("int"))
      .withColumn("is_first_purchase", expr("case when reordered = 0 then 1 else 0 end"))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Wiland-Instacart-Scala").getOrCreate()
    val basePath = if (args.length > 0) args(0) else "data"
    val goldRoot = if (args.length > 1) args(1) else "data/gold_scala"

    val t = bronze(spark, basePath)
    val li = silver(t)
    val dept = li.groupBy("department_id","department")
      .agg(count("*").alias("total_items"),
           countDistinct("user_id").alias("distinct_users"),
           avg(col("is_reorder")).alias("reorder_rate"))
    dept.write.mode("overwrite").parquet(s"$goldRoot/department_sales")

    println("Scala ETL wrote department_sales.")
  }
}
