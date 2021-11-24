from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("reviews")
sc = SparkContext(conf = conf)
data = sc.textFile("files/1800.csv")

def mapper(line):
    line = line.split(',')
    station = line[0]
    temptype = line[2]
    temp = int(line[3])
    return station, (temptype, int(temp))




rdd = data.map(mapper)
rdd = rdd.filter(lambda x: "TMIN" in x[1])
rdd = rdd.mapValues(lambda x: x[1])
rdd = rdd.reduceByKey(lambda x,y: min(x,y))




for key, value in rdd.collect():
        print (f'{key}\t{value}')