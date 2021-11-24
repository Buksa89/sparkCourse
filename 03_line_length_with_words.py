from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("lineLength")
sc = SparkContext(conf = conf)

def mapper(line):
    length = ('chars', len(line))
    words = ('words', len(line.split(' ')))
    return length, words

input = sc.textFile("files/data.txt")
mapped = input.flatMap(mapper)
reduced = mapped.reduceByKey(lambda x, y: x+y)

print("Znaki w poszczególnych wierszach:")
for key, value in mapped.collect():
    print (f'{key}: {value}')

print("\nZnaki w całym pliku")
for key, value in reduced.collect():
    print (f'{key}: {value}')






