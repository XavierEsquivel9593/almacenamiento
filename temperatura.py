#Integrantes:
#Cuevas Pena Fredy
#Esquivel Macias Erick Xavier
#Gomez Olvera Jacob Misael
#Gonzalez de Jesus Julio Alberto

#librerias
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField

#peticion get ubicados en el servidor de la api
r = requests.get('http://144.202.34.148:4002/api/sensor')
#alamacena los datos en la variable
Respuesta = r.text
#print("toda",Respuesta)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#contexto de spark representando la conexion al cluster spark
sc = spark.sparkContext

#convertir Json a dataframe
jsonStrings = [Respuesta]
#se crea un RDD en base a la coleccion de datos existentes con parallelize
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)

#dar un formato de tabla a los datos con nombre temperatura
otherPeople.printSchema()
otherPeople.createOrReplaceTempView("temperatura")
#obtener los datos de la tabla dentro del rango de fecha
spark.sql("select * from temperatura where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show
#obtener maxima, minima, media de temperatura dentro del rango de fecha
spark.sql("select max (temperatura), min (temperatura), avg (temperatura) from temperatura where fecha between '2020-12-11T03:15:45.208Z' and '2020-12-11T03:16:08.673Z'").show()

spark.stop()
