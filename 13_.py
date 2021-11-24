from pyspark import SparkConf, SparkContext
import re

WORD_RE = re.compile(r'[\w]+')

conf = SparkConf().setMaster("local").setAppName("reviews")
sc = SparkContext(conf = conf)
data = sc.textFile("files/prep_reviews_sample.tsv")
data = sc.textFile("files/prep_reviews.tsv")

def get_data_mapper(line):
    (Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,
     HelpfulnessDenominator,Score,Time,Summary,Text) = line.split('\t')
    words = WORD_RE.findall(Text)
    return None, len(words)

rdd = data.map(get_data_mapper)
rdd = rdd.reduceByKey(lambda x,y: max(x,y))


for key, value in rdd.collect():
        print (f'{key}\t{value}')