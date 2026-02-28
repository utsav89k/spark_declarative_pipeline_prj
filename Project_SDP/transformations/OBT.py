from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# OBT For GOld Layer

@dp.materialized_view(name="declarative_catalog.gold.OBT")
def OBT():

    # Reading all tables
    fact        = dp.read("declarative_catalog.gold.FacttStream")
    artist      = dp.read("declarative_catalog.gold.DimArtist")
    track       = dp.read("declarative_catalog.gold.DimTrack")
    date        = dp.read("declarative_catalog.gold.DimDate")
    user        = dp.read("declarative_catalog.gold.DimUser")

    # Joining the Tables
    obt_table = fact \
    .join(track,  fact.track_id  == track.track_id,"left")\
        .join(artist, track.artist_id == artist.artist_id, "left")\
            .join(date,   fact.date_key  == date.date_key,     "left")\
                .join(user,   fact.user_id   == user.user_id,      "left")\
                    .select(
                            fact.stream_id,
                            fact.user_id,
                            fact.track_id,
                            fact.date_key,
                            fact.listen_duration,
                            fact.device_type,
                            fact.is_carryable,
                            artist.artist_name,
                            artist.genre,
                            track.track_name,
                            track.duration_sec,
                            track.album_name,
                            track.release_year,
                            track.category,
                            date.date,
                            user.user_name,
                            user.country,
                            user.subscription_type
                        )

    return obt_table
    
 