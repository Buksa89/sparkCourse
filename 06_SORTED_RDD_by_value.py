
from pyspark import SparkConf, SparkContext
import re


conf = SparkConf().setMaster("local").setAppName("reviews")
sc = SparkContext(conf = conf)
data = sc.textFile("files/book.txt")


def mapper(line):
    
    raw_words = re.compile(r'\W+', re.UNICODE).split(line.lower())
    words = []
    for word in raw_words:
        word = word.encode('ascii','ignore')
        if (word):
            words.append(word.decode())
    return words


rdd = data.flatMap(mapper)
rdd = rdd.map(lambda x:(x,1))
rdd = rdd.reduceByKey(lambda x,y: x+y)


# Sortowanie po wartościach:
reversed_rdd = rdd.map(lambda x: (x[1],x[0]))
sorted_rdd = reversed_rdd.sortByKey().map(lambda x: (x[1],x[0]))

for key, value in sorted_rdd.collect():
    print (f'{key}\t{value}')