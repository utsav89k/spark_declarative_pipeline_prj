from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# BRONZE LAYER


# Defining the Schema
schema = StructType([
    StructField("user_id",           IntegerType(),   True),
    StructField("user_name",         StringType(),    True),
    StructField("country",           StringType(),    True),
    StructField("subscription_type", StringType(),    True),
    StructField("start_date",        DateType(),      True),
    StructField("end_date",          DateType(),      True),
    StructField("updated_at",        TimestampType(), True)
])

# Empty Streaming Target TABLE
dp.create_streaming_table("declarative_catalog.bronze.bronze_user")

# Reading the Data from Source 1
@dp.append_flow(target="declarative_catalog.bronze.bronze_user")
def first_source():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/User/first_half/")
    return df


# Reading the Data From Source 2
@dp.append_flow(target="declarative_catalog.bronze.bronze_user")
def second_source():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/User/second_half/")
    return df




# SILVER LAYER

@dp.materialized_view(name="declarative_catalog.silver.silver_user")
@dp.expect_all_or_drop({
    "user_id_not_null": "user_id IS NOT NULL",
    "user_name_not_null": "user_name IS NOT NULL"
})
def silver_user():
    silver_user_df=dp.read("declarative_catalog.bronze.bronze_user")
    silver_user_df=silver_user_df.withColumn("country", upper(col("country")))
    return silver_user_df



# GOLD LAYER

dp.create_streaming_table("declarative_catalog.gold.DimUser")


dp.create_auto_cdc_flow(
  target = "declarative_catalog.gold.DimUser",
  source = "declarative_catalog.silver.silver_user",
  keys = ["user_id"],
  sequence_by = col("updated_at"),
  except_column_list = ["updated_at"],
  stored_as_scd_type = 2
)
















