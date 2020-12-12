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

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#contexto de spark representando la conexion al cluster spark
sc = spark.sparkContext

#convertir Json obtenido del servidor de la api a dataframe
jsonStrings = [Respuesta]
#se crea un RDD(colecicon de elementos) en base a la coleccion de datos existentes con parallelize
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)

#dar un formato de tabla a los datos con nombre humedad
otherPeople.printSchema()
otherPeople.createOrReplaceTempView("humedad")
#obtener los datos de la tabla dentro del rango de fecha
spark.sql("select * from humedad where fecha between '2020-12-11T03:16:10.771Z' and '2020-12-11T23:35:50.068Z'").show()
#obtener maxima, minima, media de humedad dentro del rango de fecha
spark.sql("select max (humedad), min (humedad), avg (humedad) from humedad where fecha between '2020-12-11T03:16:10.771Z' and '2020-12-11T23:35:50.068Z'").show()
spark.stop()
