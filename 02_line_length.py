from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("lineLength")
sc = SparkContext(conf = conf)

input = sc.textFile("files/data.txt")
mapped = input.map(lambda x: ('chars', len(x)))
reduced = mapped.reduceByKey(lambda x, y: x+y)

print("Znaki w poszczególnych wierszach:")
for i in mapped.collect():
    print (i)

print("\nZnaki w całym pliku")
reduced.collect()

