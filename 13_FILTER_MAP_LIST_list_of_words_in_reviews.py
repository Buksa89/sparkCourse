from pyspark import SparkConf, SparkContext
import re

WORD_RE = re.compile(r'[\w]+')

conf = SparkConf().setMaster("local").setAppName("reviews")
sc = SparkContext(conf = conf)
data = sc.textFile("files/prep_reviews_sample.tsv")

def get_data_mapper(line):
    (Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,
     HelpfulnessDenominator,Score,Time,Summary,Text) = line.split('\t')
    words = WORD_RE.findall(Text)
    # filter filtruje tablicę
    words = filter(lambda word: len(word)>1 , words)
    # map iteruje po tablicy i robi na niej zadaną funkcję
    words = map(str.lower, words)
    return Score, list(words)

rdd = data.map(get_data_mapper)
rdd = rdd.reduceByKey(lambda x,y: x+y)

for key, value in rdd.collect():
    print (f'{key}\t{value}')