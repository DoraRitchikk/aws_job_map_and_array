from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType,StructType,StructField,DoubleType,ArrayType

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("C:/Users/Dora Ritchik/Downloads/Telegram Desktop/3_dump.parquet")
df_1 = df.select(F.col('id'),F.col('language'))

schema = ArrayType(StructType([
    StructField("l",StringType(),True),
    StructField("v",DoubleType(),True),
    StructField("p",DoubleType(),True)
])
)
language_schema = df_1.withColumn('language',F.col("language")).withColumn(
        'language', F.from_json(
                            F.col('language'),schema))
explode_language = language_schema.withColumn('language',F.explode(F.col('language')))
result_values = explode_language.withColumn('language_code',F.col('language.l')).withColumn('value',F.col('language.v')).drop('language')

w = Window.partitionBy('id').orderBy(F.col('id').asc()).orderBy(F.col('value').desc())
min_ten = result_values.withColumn('Minimum',F.min(F.col('value')).over(w)).withColumn('Row',F.row_number().over(w)).filter(F.col('Row') <= 10).show(30)