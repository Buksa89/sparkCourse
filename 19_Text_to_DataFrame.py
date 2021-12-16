from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///SparkCourse/book.txt")
# Split using a regular expression that extracts words

# words = inputDF.select(func.split(inputDF.value, "\\W+").alias("word")) #Dzieli tekst  na wiersze

words = inputDF.select(f.explode(f.split(inputDF.value, "\\W+")).alias("word")) #dzieli tekst na słowa
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(f.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())