from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

spark.sql("CREATE DATABASE IF NOT EXISTS datalake_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS datalake_gold")

class MedallionETL:
    def __init__(self, spark):
        self.spark = spark

    def extract(self, path: str, format: str = "parquet"):
        return self.spark.read.format(format).load(path)

    def transform(self, df, new_cols: dict = None):
        if new_cols:
            for col_name, expr in new_cols.items():
                df = df.withColumn(col_name, expr)
        return df

    def load(self, df, path: str, format: str = "delta", mode: str = "overwrite",
             partition_by=None, table_name=None):
        writer = df.write.format(format).mode(mode)
        if format == "delta" and mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        if table_name:
            writer.option("path", path).saveAsTable(table_name)
        else:
            writer.save(path)


etl        = MedallionETL(spark)
base_path  = "s3://tlc-export-lake"
current_date = date.today().isoformat()
fleets     = ["yellow", "green"]

casts_bronze = {
    "yellow": {
        "VendorID":        IntegerType(),
        "DOLocationID":    IntegerType(),
        "PULocationID":    IntegerType(),
        "passenger_count": LongType(),
        "RatecodeID":      LongType(),
    },
    "green": {
        "VendorID":        IntegerType(),
        "DOLocationID":    IntegerType(),
        "PULocationID":    IntegerType(),
        "passenger_count": LongType(),
        "RatecodeID":      LongType(),
        "ehail_fee":       DoubleType(),
        "payment_type":    LongType(),
        "trip_type":       LongType(),
    }
}

# Bronze
print("bronze: starting ingestion...")
for taxi in fleets:
    landing_path  = f"{base_path}/landing_zone/{taxi}_taxi/"
    landing_files = [f.path for f in dbutils.fs.ls(landing_path) if f.name.endswith(".parquet")]

    for file in landing_files:
        df_raw = spark.read.parquet(file)

        df_raw = df_raw.toDF(*[c.lower() for c in df_raw.columns])

        for col_name, col_type in casts_bronze[taxi].items():
            if col_name in df_raw.columns:
                df_raw = df_raw.withColumn(col_name, F.col(col_name).cast(col_type))
        etl.load(
            df_raw.withColumn("date_partition", F.lit(current_date)),
            f"{base_path}/bronze_layer/{taxi}_taxi/",
            format="parquet",
            mode="append",
            partition_by="date_partition"
        )

# Silver
print("silver: starting processing...")
for taxi in fleets:
    prefix    = "tpep" if taxi == "yellow" else "lpep"
    df_bronze = etl.extract(f"{base_path}/bronze_layer/{taxi}_taxi/")
    df_silver = etl.transform(df_bronze, {
        "date_partition": F.date_format(F.col(f"{prefix}_pickup_datetime"), "yyyy-MM-dd")
    })
    etl.load(
        df_silver,
        f"{base_path}/silver_layer/{taxi}_taxi/",
        format="delta",
        partition_by="date_partition",
        table_name=f"datalake_silver.{taxi}_taxi"
    )

# Gold
print("gold: merging datasets...")
df_yellow = spark.read.table("datalake_silver.yellow_taxi").withColumn("taxi_type", F.lit("yellow"))
df_green  = spark.read.table("datalake_silver.green_taxi").withColumn("taxi_type", F.lit("green"))

df_yellow = (df_yellow
    .withColumnRenamed("tpep_pickup_datetime",  "pep_pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "pep_dropoff_datetime"))

df_green = (df_green
    .withColumnRenamed("lpep_pickup_datetime",  "pep_pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "pep_dropoff_datetime"))

gold_columns = ["taxi_type", "VendorID", "passenger_count", "total_amount",
                "pep_pickup_datetime", "pep_dropoff_datetime"]

df_gold = (
    df_yellow.select(gold_columns)
    .unionByName(df_green.select(gold_columns))
    .filter(
        (F.col("total_amount")    > 0) &
        (F.col("passenger_count") > 0) &
        (F.col("pep_dropoff_datetime").isNotNull()) &
        (F.col("pep_pickup_datetime") >= "2023-01-01") & 
        (F.col("pep_pickup_datetime") < "2023-06-01")
    )
)

etl.load(
    etl.transform(df_gold, {"date_partition": F.date_format(F.col("pep_pickup_datetime"), "yyyy-MM-dd")}),
    f"{base_path}/gold_layer/taxi_consumption/",
    format="delta",
    partition_by="date_partition",
    table_name="datalake_gold.taxi_consumption"
)