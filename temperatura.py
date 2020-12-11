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
Respuesta = r.text
print("toda",Respuesta)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

jsonStrings = [Respuesta]
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)

otherPeople.printSchema()
otherPeople.createOrReplaceTempView("temperatura")
spark.sql("select * from temperatura where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show
spark.sql("select max (temperatura), min (temperatura), avg (temperatura) from temperatura where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show()

spark.stop()