from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# BRONZE LAYER


# Defining the Schema
schema = StructType([
    StructField("track_id", IntegerType(),True),
    StructField("track_name",StringType(),True),
    StructField("artist_id",IntegerType(),True),
    StructField("album_name",StringType(),True),
    StructField("duration_sec",IntegerType(),True),
    StructField("release_date",DateType(),True),
    StructField("updated_at",TimestampType(),True)
])

# Empty Streaming Target TABLE
dp.create_streaming_table("declarative_catalog.bronze.bronze_track")

# Reading the Data from Source 1
@dp.append_flow(target="declarative_catalog.bronze.bronze_track")
def first_source_track():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/Track/first_half/")
    return df


# Reading the Data From Source 2
@dp.append_flow(target="declarative_catalog.bronze.bronze_track")
def second_source_track():
    df=spark.readStream.format("csv").option("header", "true").schema(schema) \
    .load("s3://declarative-pipeline-uk/Data/Track/second_half/")
    return df



# SILVER LAYER

@dp.materialized_view(name="declarative_catalog.silver.silver_track")
@dp.expect_all_or_drop({
    "track_id_not_null": "track_id IS NOT NULL",
    "track_name_not_null": "track_name IS NOT NULL"
})

def silver_track():
    silver_track_df=dp.read("declarative_catalog.bronze.bronze_track")
    silver_track_df=silver_track_df.withColumn("release_year",year(col("release_date")))\
        .withColumn("duration_min",round((col("duration_sec")/60),2))\
            .withColumn("category",split(col("album_name"),' ')[0])
    
    # Adjusting the Columns
    silver_track_df = silver_track_df.select(
        "track_id",
        "track_name",
        "artist_id",
        "album_name",
        "duration_sec",
        "release_date",  
        "category",
        "release_year",       
        "duration_min",   
        "updated_at"      
    )
    
    return silver_track_df




# GOLD LAYER
dp.create_streaming_table("declarative_catalog.gold.DimTrack")


def gold_track():
    gold_track_df=dp.read("declarative_catalog.silver.silver_track")
    gold_track_df = gold_track_df.withColumn(
        "is_recent",
        when(year(col("release_date")) >= 2020, lit(True))
        .otherwise(lit(False))
    )
    return gold_track_df

dp.create_auto_cdc_flow(
  target = "declarative_catalog.gold.DimTrack",
  source = "declarative_catalog.silver.silver_track",
  keys = ["track_id"],
  sequence_by = col("updated_at"),
  except_column_list = ["updated_at"],
  stored_as_scd_type = 2
)







