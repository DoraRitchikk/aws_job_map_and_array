from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import MapType,StringType,StructType,StructField,LongType,DoubleType


spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("C:/Users/Dora Ritchik/Downloads/Telegram Desktop/3_dump.parquet")

df_sort = df.select(F.col("location")).withColumn('stats Cities',F.from_json(F.col('location'),MapType(StringType(),StringType())))

df_with_schema = df_sort.select(df_sort['stats Cities'].getItem('statsCities')).withColumn(
        'statsCities', F.from_json(F.col('stats Cities[statsCities]'), MapType(
                keyType=StringType(),
                valueType=MapType(
                    keyType=StringType(),
                    valueType=MapType(
                        keyType=StringType(),
                        valueType=MapType(StringType(),StringType()))
                )
            )
        )).select("statsCities")
def flatten_stats_cities_to_cities():
    return F.flatten(
        F.transform(
            F.flatten(
                F.transform(
                    F.map_values('statsCities'),
                    lambda regions: F.transform(
                        F.map_values(regions),
                        lambda city: F.transform_values(city, lambda _, value: value.percent)
                    ),
                )
            ),
            lambda city_map: F.map_entries(city_map)
        )
    )

df = df_with_schema.withColumn('cities', flatten_stats_cities_to_cities())

df.select('cities').show(10, False)