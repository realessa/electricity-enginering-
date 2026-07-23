# -*- coding: utf-8 -*-
"""Delta Lakehouse: Bronze -> Silver -> Gold with a real MERGE and proof of schema enforcement."""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

BRONZE_PATH = "./bronze"
SILVER_PATH = "./silver"
GOLD_PATH = "./gold"

READING_SCHEMA = StructType([
    StructField("meter_id", StringType(), False),
    StructField("building", StringType(), False),
    StructField("consumption_kwh", DoubleType(), False),
    StructField("timestamp", TimestampType(), False),
])


def get_spark():
    import os

    os.environ["HADOOP_HOME"] = "C:\\"
    os.environ["hadoop.home.dir"] = "C:\\"

    builder = (
        SparkSession.builder
        .appName("ElectricityPipeline")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


from datetime import datetime

def build_bronze(spark, records):

    for r in records:
        if isinstance(r.get("timestamp"), str):
            r["timestamp"] = datetime.fromisoformat(
                r["timestamp"]
            )

    df = spark.createDataFrame(
        records,
        schema=READING_SCHEMA
    )

    return df


def prove_schema_enforcement(spark):
    """
    Demonstrate Delta schema enforcement.
    Invalid schema must be rejected.
    """

    print("\n--- Schema Enforcement Test (expected to fail) ---")

    bad_data = [{
        "meter_id": "M999",
        "building": "Building_X",
        "consumption_kwh": "not_a_number",
        "timestamp": datetime.now(),
        "extra_col": "unexpected_field"
    }]

    bad_df = spark.createDataFrame(bad_data)

    try:
        bad_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "false") \
            .save(SILVER_PATH)

        print("❌ Schema enforcement FAILED - bad data was accepted")

    except Exception as e:
        print("✅ Schema enforcement PASSED - invalid schema rejected")
        print(str(e)[:300])

def build_silver(spark, bronze_df):
    silver_df = (
        bronze_df.dropDuplicates(["meter_id", "timestamp"])
        .filter(col("meter_id").isNotNull())
        .filter(col("building").isNotNull())
        .filter(col("consumption_kwh") > 0)
    )
    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print("✅ Silver Layer Created")
    return spark.read.format("delta").load(SILVER_PATH)


def merge_into_silver(spark, new_records):
    """Real upsert keyed on meter_id (business key)."""
    update_df = spark.createDataFrame(new_records, schema=READING_SCHEMA)
    delta_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        delta_table.alias("target")
        .merge(update_df.alias("source"), "target.meter_id = source.meter_id")
        .whenMatchedUpdate(set={
            "building": "source.building",
            "consumption_kwh": "source.consumption_kwh",
            "timestamp": "source.timestamp",
        })
        .whenNotMatchedInsert(values={
            "meter_id": "source.meter_id",
            "building": "source.building",
            "consumption_kwh": "source.consumption_kwh",
            "timestamp": "source.timestamp",
        })
        .execute()
    )
    print("✅ MERGE into Silver complete")
    return spark.read.format("delta").load(SILVER_PATH)


def build_gold(spark, silver_df):
    gold_df = silver_df.groupBy("building").agg(
        count("meter_id").alias("total_readings"),
        avg("consumption_kwh").alias("average_consumption"),
        spark_sum("consumption_kwh").alias("total_consumption"),
        spark_max("consumption_kwh").alias("max_consumption"),
        spark_min("consumption_kwh").alias("min_consumption"),
    )
    gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
    print("✅ Gold Layer Created")
    return spark.read.format("delta").load(GOLD_PATH)


def run_lakehouse(valid_records):
    spark = get_spark()

    records = [
        r if isinstance(r, dict)
        else r.model_dump(mode="python")
        for r in valid_records
    ]

    # 1- Bronze
    bronze_df = build_bronze(spark, records)

    # 2- Create Silver first
    silver_df = build_silver(spark, bronze_df)

    # 3- Test schema enforcement on Silver
    prove_schema_enforcement(spark)

    # 4- MERGE
    new_data = [
        {
            "meter_id": "M001",
            "building": "Building_A",
            "consumption_kwh": 45.3,
            "timestamp": datetime.now()
        },
        {
            "meter_id": "M005",
            "building": "Building_E",
            "consumption_kwh": 27.8,
            "timestamp": datetime.now()
        },
    ]

    silver_df = merge_into_silver(spark, new_data)

    # 5- Gold
    gold_df = build_gold(spark, silver_df)

    return spark, bronze_df, silver_df, gold_df


if __name__ == "__main__":
    from dags.producer import electricity_data
    from dags.schemas import ElectricityReading

    sample_valid = [ElectricityReading(**r) for r in electricity_data if r["meter_id"] and r["consumption_kwh"] > 0]
    run_lakehouse(sample_valid)
