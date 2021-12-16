from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)
data = sc.wholeTextFiles("files/example_2.json")
import json
data = data.map(lambda x: json.loads(x[1]))

data.foreach(lambda x: print(x))

sc.stop()