from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# BRONZE LAYER


# Defining the Schema
schema = StructType([
    StructField("artist_id", IntegerType(),True),
    StructField("artist_name",StringType(),True),
    StructField("genre",StringType(),True),
    StructField("country",StringType(),True),
    StructField("updated_at",TimestampType(), True)
])

# Empty Streaming Target TABLE
dp.create_streaming_table("declarative_catalog.bronze.bronze_artist")

# Reading the Data from Source 1
@dp.append_flow(target="declarative_catalog.bronze.bronze_artist")
def first_source_artist():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/Artist/first_half/")
    return df


# Reading the Data From Source 2
@dp.append_flow(target="declarative_catalog.bronze.bronze_artist")
def second_source_artist():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/Artist/second_half/")
    return df


# SILVER LAYER

@dp.materialized_view(name="declarative_catalog.silver.silver_artist")
@dp.expect_all_or_drop({
    "artist_id_not_null": "artist_id IS NOT NULL",
    "artist_name_not_null": "artist_name IS NOT NULL"
})
def silver_artist():
    silver_artist_df=dp.read("declarative_catalog.bronze.bronze_artist")
    silver_artist_df=silver_artist_df.withColumn("country", upper(col("country")))
    return silver_artist_df





# GOLD LAYER
dp.create_streaming_table("declarative_catalog.gold.DimArtist")

def gold_artist():
    gold_artist_df=dp.read("declarative_catalog.silver.silver_artist")
    return gold_artist_df

dp.create_auto_cdc_flow(
  target = "declarative_catalog.gold.DimArtist",
  source = "declarative_catalog.silver.silver_artist",
  keys = ["artist_id"],
  sequence_by = col("updated_at"),
  except_column_list = ["updated_at"],
  stored_as_scd_type = 2
)














