from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)

data = sc.textFile("files/callsigns")

# Utwórz akumulatory sprawdzające poprawność znaczników wywołań

validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)

def validateSign(sign):

    global validSignCount, invalidSignCount
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False

# Policz, ile razy kontaktowaliśmy się z każdym znacznikiem wywołania
validSigns = data.filter(validateSign)
contactCount = validSigns.map(lambda sign: (sign, 1)).reduceByKey(lambda x, y: x + y)

# Wymuś obliczenie, aby liczniki zostały rozpropagowane
contactCount.count()

if invalidSignCount.value < 1 * validSignCount.value:
    contactCount.foreach(lambda x: print(x))

else:
    print (f"Too many errors: {invalidSignCount.value}, {validSignCount.value} valid")

sc.stop()