# FUNCTIONS https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html
# first - zwraca row
# Join alternative

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("files/Marvel-names.txt")

lines = spark.read.text("files/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", f.split(f.trim(f.col("value")), " ")[0]) \
     .withColumn("connections", f.size(f.split(f.trim(f.col("value")), " ")) - 1) \
     .groupBy("id").agg(f.sum("connections").alias("connections"))

    
mostPopular = connections.sort(f.col("connections").desc()).first()

mostPopularName = names.filter(f.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

spark.stop()