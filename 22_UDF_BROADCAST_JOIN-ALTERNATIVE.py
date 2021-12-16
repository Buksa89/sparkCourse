from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

# Zdefiniowanie zwykłego słownika
def loadMovieNames():
    movieNames = {}
    with codecs.open("files/ml-100k/u.ITEM", "r", encoding='utf-8', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Przypisanie słownika do zmiennej typu broadcast
nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("files/ml-100k/u.data")
movieCounts = moviesDF.groupBy("movieID").count()

#Odwołanie się do zmiennej typu broadcast:
# print(nameDict.value)

# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = f.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(f.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(f.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
