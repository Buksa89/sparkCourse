from pyspark import SparkConf, SparkContext
import numpy as np
from pyspark import statcounter

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)
data = sc.textFile("files/rekordy/rekordy/block_1.csv")
# data = sc.textFile("files/rekordy/rekordy/block_1_sample.csv")

### Wstępne przygotowanie RDD
def parse(line):
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

### Funkcje customowe

def mystats(rdd):
    """ Funkcja działająca jak .stats(), ale zliczająca też wartości Nan i wykluczająca je z wyniku """
    notnan = rdd.filter(lambda x: not np.isnan(x))
    notnan.cache()

    count = rdd.count()
    missing = count - notnan.count()
    if count != missing:
        mean = notnan.mean()
        stdev = notnan.stdev()
        max = notnan.max()
        min = notnan.min()
    else: 
        mean = np.nan
        stdev = np.nan
        max = np.nan
        min = np.nan

    notnan.unpersist()

    return {'stats': {'count': count,  
    'mean': mean, 'stdev': stdev, 'max': max, 'min': min}, 'missing': missing}

def createStats(block_rdd):
    """Funckaj stworzona typowo pod zadane dane. Leci po wszystkich kolumnach scores i dla każdej tworzy statystyki"""
    statrdd = []
    for i in range(0,9):
        statrdd.append(mystats(block_rdd.map(lambda md: md['scores'][i])))
    return statrdd

###

# Przsygotowanie statystyk osobno dla danych prawidłowych i nieprawidłowych i utworzenie z nich rdd
matched = parsed.filter(lambda x: x['matched'])
notmatched = parsed.filter(lambda x: not x['matched'])
statsm = sc.parallelize(createStats(matched))
statsn = sc.parallelize(createStats(notmatched))

# Sumuję brakujące dane z każdej kolumny
# Sprawdzam różnicę średniej z każdej kolumny
# Jeśli brakujących danych jest dużo, kolumna jest słabym wskaźnikiem
# Jeśli Średnia się nieznacznie różni, czyli różnica jest bliska zeru to kolumna też niewiele mówi.
# NAjlepsze są kolumny gdzie jest mało brakującychc danych, a różnica średnich jest jak największa
statsm.zip(statsn) \
      .map(lambda x: (x[0]['missing'] + x[1]['missing'], x[0]['stats']['mean'] - x[1]['stats']['mean']) ) \
      .foreach(print)


sc.stop()
