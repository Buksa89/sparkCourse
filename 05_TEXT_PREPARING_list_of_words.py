from pyspark import SparkConf, SparkContext
import re


conf = SparkConf().setMaster("local").setAppName("reviews")
sc = SparkContext(conf = conf)
data = sc.textFile("files/book.txt")


def mapper(line):
    
    raw_words = re.compile(r'\W+', re.UNICODE).split(line.lower())
    words = []
    #UNCODING
    for word in raw_words:
        word = word.encode('ascii','ignore')
        if (word):
            words.append(word.decode())
    return words


rdd = data.flatMap(mapper)

for key in rdd.collect():
    print (key)