from pyspark import SparkConf, SparkContext
import numpy as np
from pyspark import statcounter

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)
data = sc.textFile("files/rekordy/rekordy/block_1_sample.csv")
# data = sc.textFile("out")
# data = sc.parallelize(data.take(10))

def parse(line):
    # (id_1,id_2,cmp_fname_c1,cmp_fname_c2,cmp_lname_c1,cmp_lname_c2,cmp_sex,cmp_bd,cmp_bm,cmp_by,cmp_plz,is_match) = line.split(',')
    fields = line.split(',')
    id_1 = int(fields[0])
    id_2 = int(fields[1])
    scores = list(map(lambda x: np.double(x) if x !='?' else np.nan, fields[2:11]))
    matched = eval(fields[11].capitalize())

    return {"id_1":id_1, "id_2":id_2, "scores":scores, "matched":matched}

def isHeader(line):
    return 'id_1' in line

data = data.filter(lambda line: not isHeader(line))
parsed = data.map(parse)
parsed.cache()
grouped = parsed.groupBy(lambda x: x['matched']) \
                .mapValues(lambda x: len(x))

print("Wyswietlenie liczby rekordow trafnych i nietrafnych:")
grouped.foreach(lambda x:print(x))

#######

print("Statystyki dla jednej z kolumn w scores")
print(parsed.map(lambda md: md['scores'][1]).stats())
# Wykluczam NaN psujace wynik
print(parsed.map(lambda md: md['scores'][1]).filter(lambda x: not np.isnan(x)).stats())

sc.stop()