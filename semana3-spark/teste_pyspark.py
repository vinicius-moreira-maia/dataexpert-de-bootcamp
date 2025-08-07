import pyspark
from pyspark.sql import SparkSession

# com spark-submit eu não preciso encerrar a sessão, isso é automático

#spark-submit teste_pyspark.py

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df.show()

#para testar a escrita:
#df.write.parquet('zones')