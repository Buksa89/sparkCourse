from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("wordCounter")
sc = SparkContext(conf = conf)
input = sc.textFile("files/iliada.txt")

words = input.flatMap(lambda x: re.compile(r'\W+', re.UNICODE).split(x.lower()))

mapped = words.map(lambda x: (x,1))
reduced = mapped.reduceByKey(lambda x,y: x+y)
## Znalezienie maksymalnej wartosci:
reversed = reduced.map(lambda x: (None, (x[1],x[0])))
only_max_reduced = reversed.reduceByKey(lambda x, y: max(x, y))

print (only_max_reduced.collect())

