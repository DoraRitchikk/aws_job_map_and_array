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


Countries=df_with_schema.select(F.map_values('statsCities').alias('Countries'))
Countries_exploded = Countries.select(F.explode(Countries['Countries']).alias('Countries'))


sities=Countries_exploded.select(F.map_values(Countries_exploded['Countries']).alias('Sities'))
sities_exploded = sities.select(F.explode(sities['Sities']).alias('Sities'))
#GET KEYS FROM COUNTRIES

#Get keys
Sity_keys=sities_exploded.withColumn('Sities_key',F.map_keys(sities_exploded['Sities']))
Sity_keys_exploded = Sity_keys.withColumn('Sities_Key',F.explode(Sity_keys['Sities_key']))

#Get percent
df_properties = Sity_keys_exploded.withColumn('Percent',F.map_values(Sity_keys_exploded['Sities']))
df_properties_exploded = df_properties.withColumn('Percent',F.explode(df_properties['Percent']))
df_extract_properties = df_properties_exploded.withColumn('Percent',F.map_values(df_properties_exploded['Percent']))
df_percent = df_extract_properties.withColumn('Percent',df_extract_properties['Percent'].getItem(1))

df_result = df_percent.select(F.array(df_percent['Sities_Key'],df_percent['percent']).alias('Result')).where(F.col('Sities_Key') != "UNKNOWN").show()
