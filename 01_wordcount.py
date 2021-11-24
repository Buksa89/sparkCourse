from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCounter")
sc = SparkContext(conf = conf)

def parse(text):
    words = text.split(' ')
    return words


#text = sc.parallelize("Mary has a cat cat is black") # po literce
text = sc.parallelize(["Mary has a cat cat is black",'cats not exist']) # po wyrazie
parsed = text.flatMap(parse)
mapped = parsed.map(lambda x: (x,1))
reduced = mapped.reduceByKey(lambda x,y: x+y)

for i in reduced.collect():
    print (i)
