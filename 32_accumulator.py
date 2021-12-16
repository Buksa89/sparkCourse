from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)

# data = sc.wholeTextFiles("files/example_2.json")
# data = sc.textFile("files/animals.csv")

data = sc.textFile("files/data.txt")

# Utwórz Accumulator[Int] z początkową wartością 0

blankLines = sc.accumulator(0)
lines = sc.accumulator(0)

def extractCallSigns(line):
    global blankLines # Spraw, aby zmienna globalna była dostępna
    global lines
    lines += 1
    if (line == ""):
        blankLines += 1
    return line.split(" ")

callSigns = data.flatMap(extractCallSigns)

callSigns.foreach(lambda x: print(x))

print (f"Blank lines: {blankLines.value}")
print (f"lines: {lines.value}")



sc.stop()