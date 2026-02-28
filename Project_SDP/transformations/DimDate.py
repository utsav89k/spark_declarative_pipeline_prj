from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# BRONZE LAYER


# Defining the Schema
schema = StructType([
    StructField("date_key", IntegerType(), True),
    StructField("date",     DateType(),    True),
    StructField("day",      IntegerType(), True),
    StructField("month",    IntegerType(), True),
    StructField("year",     IntegerType(), True),
    StructField("weekday",  StringType(),  True)
])

# Empty Streaming Target TABLE
dp.create_streaming_table("declarative_catalog.bronze.bronze_date")

# Reading the Data from Source 1
@dp.append_flow(target="declarative_catalog.bronze.bronze_date")
def first_source_date():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/OriginalDate/")
    return df


# Reading the Data From Source 2
@dp.append_flow(target="declarative_catalog.bronze.bronze_date")
def second_source_date():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/OriginalDate2/")
    return df




# SILVER LAYER

@dp.materialized_view(name="declarative_catalog.silver.silver_date")
@dp.expect_or_drop("date_key_not_null", "date_key IS NOT NULL")
@dp.expect("full_date_present", "date IS NOT NULL")

def silver_date():
    silver_date_df=dp.read("declarative_catalog.bronze.bronze_date")

    # Tranformation
    silver_date_df = silver_date_df.withColumn("month_name",date_format(col("date"), "MMMM")) \
           .withColumn("is_weekend", col("weekday").isin("Saturday", "Sunday")) \
           .withColumn("start_of_quarter",to_date(date_trunc("quarter", col("date"))))

    return silver_date_df



# GOLD LAYER
dp.create_streaming_table("declarative_catalog.gold.DimDate")

def gold_date():
    gold_date_df=dp.read("declarative_catalog.silver.silver_date")
    gold_date_df=gold_date_df.withColumn(
        "week_of_year",
        weekofyear(col("date"))
    )

    return gold_date_df



dp.create_auto_cdc_flow(
  target = "declarative_catalog.gold.DimDate",
  source = "declarative_catalog.silver.silver_date",
  keys = ["date_key"],
  sequence_by = col("date"),
  stored_as_scd_type = 1
)






