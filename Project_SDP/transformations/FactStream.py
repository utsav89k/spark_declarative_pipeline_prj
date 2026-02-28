from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# BRONZE LAYER


# Defining the Schema
schema = StructType([
    StructField("stream_id",LongType(), True),
    StructField("user_id",  IntegerType(),True),
    StructField("track_id", IntegerType(),True),
    StructField("date_key", IntegerType(),True),
    StructField("listen_duration",IntegerType(),True),
    StructField("device_type",StringType(),True),
    StructField("stream_timestamp",TimestampType(),True)
])

# Empty Streaming Target TABLE
dp.create_streaming_table("declarative_catalog.bronze.bronze_FACTT")

# Reading the Data from Source 1
@dp.append_flow(target="declarative_catalog.bronze.bronze_FACTT")
def first_source_factt():
    fact_df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/Fact/first_half/")
    return fact_df


# Reading the Data From Source 2
@dp.append_flow(target="declarative_catalog.bronze.bronze_FACTT")
def second_source_factt():
    fact_df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/Fact/second_half/")
    return fact_df



# SILVER LAYER

@dp.materialized_view(name="declarative_catalog.silver.silver_factt")

def silver_factt():
    
    # Reading the Bronze Table
    silver_fact_df=dp.read("declarative_catalog.bronze.bronze_factt")

    # Tranformation
    silver_fact_df = silver_fact_df.withColumn("is_carryable", when(col("device_type") == "Desktop", "No")\
    .when((col("device_type") == "Mobile") | (col("device_type") == "Smart Speaker"), "Yes")\
    .otherwise("No Data Found"))

    # Arranging the Columns
    silver_fact_df = silver_fact_df.select(
        "stream_id",
        "user_id",
        "track_id",
        "date_key",
        "listen_duration",
        "device_type",
        "is_carryable",       
        "stream_timestamp"    
        )

    return silver_fact_df


# GOLD LAYER
dp.create_streaming_table("declarative_catalog.gold.FacttStream")


dp.create_auto_cdc_flow(
  target = "declarative_catalog.gold.FacttStream",
  source = "declarative_catalog.silver.silver_factt",
  keys = ["stream_id"],
  sequence_by = col("stream_timestamp"),
  except_column_list = ["stream_timestamp"],
  stored_as_scd_type = 2
)




