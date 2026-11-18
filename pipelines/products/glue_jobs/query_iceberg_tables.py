from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_TABLE = "lakehouse.silver.products"
GOLD_TABLE = "lakehouse.gold.product_category_summary"


def build_spark():
    return (
        SparkSession.builder
        .appName("ucv-query-iceberg")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse",
                "s3a://ucv-data-platform/products")
        .config("spark.hadoop.fs.s3a.endpoint", "http://floci:4566")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def check(label, ok, detail):
    print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")


spark = build_spark()
spark.sparkContext.setLogLevel("ERROR")

silver = spark.table(SILVER_TABLE)
gold = spark.table(GOLD_TABLE)

print("\n=== SILVER: esquema ===")
silver.printSchema()
print("=== SILVER: muestra de filas ===")
silver.show(10, truncate=False)
print("Silver rows:", silver.count())

print("\n=== VALIDACION SILVER (Capturas 4 y 5) ===")
null_ids = silver.filter(F.col("product_id").isNull()).count()
bad_prices = silver.filter(~(F.col("price") > 0)).count()
dups = (
    silver.groupBy("product_id").count().filter(F.col("count") > 1).count()
)
check("product_id no nulo", null_ids == 0, f"{null_ids} nulos")
check("price > 0", bad_prices == 0, f"{bad_prices} filas invalidas")
check("product_id sin duplicados", dups == 0, f"{dups} ids duplicados")

print("\n=== GOLD: esquema ===")
gold.printSchema()
print("=== GOLD: metricas por categoria ===")
gold.show(50, truncate=False)
gold_rows = gold.count()
print("Gold rows:", gold_rows)

print("\n=== VALIDACION GOLD (Capturas 6 y 7) ===")
distinct_cat = gold.select("category").distinct().count()
bad_counts = gold.filter(~(F.col("product_count") > 0)).count()
bad_avg = gold.filter(~(F.col("average_price") > 0)).count()
null_cat = gold.filter(F.col("category").isNull()).count()
check("una fila por category", gold_rows == distinct_cat,
      f"{gold_rows} filas / {distinct_cat} categorias")
check("category no nula", null_cat == 0, f"{null_cat} nulos")
check("product_count > 0", bad_counts == 0, f"{bad_counts} filas invalidas")
check("average_price > 0", bad_avg == 0, f"{bad_avg} filas invalidas")

spark.stop()