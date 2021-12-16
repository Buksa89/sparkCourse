from pyspark import SparkConf, SparkContext
import numpy as np
from pyspark import statcounter

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)
data = sc.textFile("files/rekordy/rekordy/block_1.csv")
# data = sc.textFile("files/rekordy/rekordy/block_1_sample.csv")

### Wstępne przygotowanie RDD. 
# Tym razem NaNy przeksztalcam na 0

def parse(line):
    fields = line.split(',')
    id_1 = int(fields[0])
    id_2 = int(fields[1])
    scores = list(map(lambda x: np.double(x) if x !='?' else 0, fields[2:11]))
    matched = eval(fields[11].capitalize())

    return {"id_1":id_1, "id_2":id_2, "scores":scores, "matched":matched}

def isHeader(line):
    return 'id_1' in line

data = data.filter(lambda line: not isHeader(line))
parsed = data.map(parse)
parsed.cache()
grouped = parsed.groupBy(lambda x: x['matched']) \
                .mapValues(lambda x: len(x))

### Funkcje customowe

def createStats(block_rdd):
    """Funkcja stworzona typowo pod zadane dane. Leci po wszystkich kolumnach scores i dla każdej tworzy statystyki"""
    statrdd = []
    for i in range(0,9):
        statrdd.append(block_rdd.map(lambda md: md['scores'][i]).stats())
    return statrdd

###

# Przsygotowanie statystyk osobno dla danych prawidłowych i nieprawidłowych i utworzenie z nich rdd
# matched = parsed.filter(lambda x: x['matched'])
# notmatched = parsed.filter(lambda x: not x['matched'])
# statsm = sc.parallelize(createStats(matched))
# statsn = sc.parallelize(createStats(notmatched))



# Tworzę pary k-v, gdzie
# key to True albo False z kolumny "matched" - określa czy rekordy są trafione
# value to suma wartości najlepiej rokujących kolumn ze scores
ct = parsed.map(lambda x: (x['matched'], sum(map((lambda i: x['scores'][i]), [2,5,6,7,8]))))
# ct.foreach(print)
ct.persist()

#print('realna ilosc rekordów zgodnych i niezgodnych:')
print(ct.countByKey())
#print('wyliczona ilosc rekordow zgodnych i niezgodnych z marginesem 4/5:')
print(ct.filter(lambda s: s[1] >= 4.0).countByKey())
#print('wyliczona ilosc rekordow zgodnych i niezgodnych z marginesem 2/5:')
print(ct.filter(lambda s: s[1] >= 2.0).countByKey())

sc.stop()
