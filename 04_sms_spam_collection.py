from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCounter")
sc = SparkContext(conf = conf)
input = sc.textFile("files/SMSSpamCollection.txt")

def mapper(line):
    chars = ('chars', len(line))
    words = ('words', len(line.split()))
    line_num = ('line', 1)
    return line_num, words, chars


mapped = input.flatMap(mapper)
reduced = mapped.reduceByKey(lambda x,y: x+y)

for key, value in reduced.collect():
    print (f'{key}: {value}')