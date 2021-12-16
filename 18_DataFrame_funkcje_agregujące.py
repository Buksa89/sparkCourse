from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("files//fakefriends-header.csv")

people = people.select("age",'friends')
#Bez "Friends" w avg, policzyłoby średnią dla wszystkich kolumn
people.groupBy("age").avg("friends").show()

people.groupBy("age").avg("friends").sort("age").show()
people.groupBy("age").agg(f.round(f.avg("friends"),2)).sort("age").show()

people.groupBy("age").agg(f.round(f.avg("friends"),2).alias('friends')).sort("age").show()

spark.stop()

