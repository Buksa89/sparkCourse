from pyspark import SparkConf, SparkContext
import csv
from io import StringIO

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)

data = sc.textFile("files/animals.csv")

def loadRecord(line):
    input = StringIO(line)
    reader = csv.DictReader(input, fieldnames=["name",
    "body","brain"])
    return next(reader)

data = data.map(loadRecord)

data.foreach(lambda x: print(x))

# zapisanie do pliku

def writeRecords(records):
  output = StringIO()
  writer = csv.DictWriter(output, fieldnames=["name",
    "body","brain"])
  for record in records:
    writer.writerow(record)
  return [output.getvalue()]

data.mapPartitions(writeRecords).saveAsTextFile("plik")



sc.stop()