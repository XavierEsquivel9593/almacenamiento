#Integrantes:
#Cuevas Pena Fredy
#Esquivel Macias Erick Xavier
#Gomez Olvera Jacob Misael
#Gonzalez de Jesus Julio Alberto
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField

r = requests.get('http://144.202.34.148:4002/api/sensor')
response = r.text
print("toda",response)


    # $example on:init_session$
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    # $example off:init_session$

sc = spark.sparkContext


# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string
jsonStrings = [response]
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)

otherPeople.printSchema()
otherPeople.createOrReplaceTempView("humedad")
teenager = spark.sql("select * from humedad where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'")
spark.sql("select max (humedad) from humedad where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show()
spark.sql("select max (temperatura) from humedad where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show()
spark.sql("select min (humedad) from humedad").show()
spark.sql("select min (temperatura) from humedad").show()
spark.sql("select avg (humedad) from humedad").show()
spark.sql("select avg (temperatura) from humedad").show()
#spark.sql("select max (humedad), min (humedad), avg (humedad) from humedad where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show()
#spark.sql("select max (temperatura), min (temperatura), avg (temperatura) from humedad where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show()
teenager.show(100)

spark.stop()