# spark-shell --master yarn-client #Uruchomienie powłoki w klastrze
# spark-shell --master local[*] # Uruchomienie powłoki lokalnie. Zamiast * ilość wątków
# pyspark # uruchomienie powloki
# więcej arumentów, np driver-memory: https://spark.apache.org/docs/latest/running-on-yarn.html
"""
spark-submit:

--master
spark://host:port   Połączenie z klastrem Spark Standalone na określonym porcie. Domyślnie proces główny Spark Standalone używa portu 7077.
mesos://host:port   Połączenie z klastrem Mesos na określonym porcie. Domyślnie proces główny Mesos używa portu 5050.
yarn    Połączenie z klastrem YARN. Gry uruchamiamy YARN, musimy tak ustawić zmienną środowiskową HADOOP_CONF_DIR, aby wskazywała lokalizację katalogu konfiguracyjnego Hadoop, który zawiera informacje o klastrze.
local   Uruchomienie w trybie lokalnym z jednym rdzeniem.
local[N]    Uruchomienie w trybie lokalnym z N-rdzeniami.
local[*]    Uruchomienie w trybie lokalnym i wykorzystanie tylu rdzeni, ile jest dostępnych na maszynie.

--master    Wskazuje menedżerowi klastrów, aby się połączył. Opcje tego znacznika opisano w tabeli 7.1.
--deploy-mode   Tryb uruchomiania programu sterownika: lokalnie („client”) lub z maszyn roboczych w klastrze („cluster”). W trybie klienta spark-submit uruchomi nasz sterownik na tej samej maszynie, na której został uruchomiony sam spark-submit. W trybie klastra sterownik będzie przekazany do wykonania na węźle roboczym klastra. Domyślnym ustawieniem jest tryb klienta.
--class Klasa „main” naszej aplikacji, jeśli uruchamiamy program w Javie lub w Scali.
--name  Nazwa aplikacji czytelna dla człowieka. Zostanie wyświetlona w sieciowym interfejsie użytkownika Sparka.
--jars  Lista plików JAR do załadowania i umieszczenia na ścieżce klasy naszej aplikacji. Jeśli aplikacja zależy od kilku JAR innych producentów, można je dodać.
--files Lista plików do umieszczenia w katalogu roboczym naszej aplikacji. Można ich używać dla plików danych, które chcemy rozdzielić między węzły.
--py-files  Lista plików do dodania do ścieżki PYTHONPATH naszej aplikacji. Może zawierać pliki .py, .egg lub .zip.
--executormemory    Ilość pamięci dla wykonawców liczona w bajtach. Przyrostki mogą być używane do określenia większych ilości jak „512m” (512 megabajtów) lub „15g” (15 gigabajtów).
--drivermemory  Ilość pamięci dla procesu sterownika liczona w bajtach. Przyrostki mogą być używane do określenie większych ilości jak „512m” (512 megabajtów) lub „15g” (15 gigabajtów).

conf = new SparkConf()
conf.set("spark.app.name", "My Spark App")
conf.set("spark.master", "local[4]")
conf.set("spark.ui.port", "36000")

spark-submit \
--class com.example.MyApp \
--master local[4] \
--name "My Spark App" \
--conf spark.ui.port=36000 \

spark-submit \
--class com.example.MyApp \
--properties-file my-config.conf \
myApp.jar
## Zawartość my-config.conf ##
spark.master    local[4]
spark.app.name  "My Spark App"
spark.ui.port   36000

"Poznajemy sparka" tabela 8.1 opcje konfiguracyjne

"""

from pyspark import SparkConf, SparkContext
from pyspark import sql
conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)
sc.stop()

Array, data2, funkcja, key, f = ''
# tworzenie RDD:
data = sc.textFile("sciezka do pliku lub katalogu", 4)
data = sc.wholeTextFiles('ścieżkado katalogu') # wczytuje cał3 pliki jako pojedyncze wiersze rdd, gdzie key to sciezka pliku, a value to jego tresc
data = sc.parallelize(Array(1, 2, 2, 4), 4) #4 na koncu oznacza ilosc partyycji na jakie podzielic dane
data = sc.textFile("hdfs:///zadana/scieżka.txt") #HDFS
data = data.filter(lambda row: row != data.first()) #usuniecie naglowka

# transformacje:
data.filter()
data.filter(lambda line: 'id_1' in line)
data.distinct() # usuwa duplikaty
data.union(data2)   # łączenie rdd przez doklejenie kolejnych wierszy
data.intersection(data2) # część wspólna dwóch rdd (usuwa duplikaty)
data.subtract(data2) # data1-data2
data.cartesian(data2) # paruje kazdy element z kazdym
data.sample(False, 0.5) # Próbkowanie (druga wartosc to procent zbioru)
data.fold() #??????????????????????????????????????????
data.aggregate() #???????????????????????????????
data.map()
data.reduce()
#transformacje key-value:
data.reduceByKey(funkcja)
data.groupByKey()
# data.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)
data.mapValues()
data.flatMapValues(funkcja)
data.keys()
data.values()
data.sortByKey() # Jako
data.sortBy(lambda x:x[0]) #jw
data.reduceByKey() # jako drugi partametr mozna podac ilość partycji na jakich powinno odbywac sie liczenie
data.foldByKey() #????

# partycje:
data.repartition() # repartycjonowanie
data.coalesce() # jw, ale unika przenoszenia danych, wiec dziala szybciej. Mozna tylko zmniejszyc liczbe partycji
data.getNumPartitions() #sprawdza ilosc partycji

# transformacje dwóch rdd key-value
data.subtractByKey(data2) # Usuwa element z kluczem znajdującym się w innym RDD.
data.join(data2) # join części wspólnej
data.rightOuterJoin(data2) # join wszystkich elementow (wstawia wartosc None dla pustych pol)
data.fullOuterJoin(data2) # jw
data.join(data2) # join części wspólnej
data.join(data2) # join części wspólnej
data.cogroup(data2) # ??????????????
data = data.cogroup(data2).mapValues(lambda x: (list(x[0]), list(x[1]))) # laczy zbiory, podaje wyniki jako krotke dwoch list

# Akcje
# wyswietlenie RDD
data.first()
data.take(10)
data.collect()
data.top(2) # Pobiera dwie najwyzsze wartosci.
data.takeOrdered(2, funkcja) #Zwraca num elementów według podanego porządku.
data.takeSample(False,2) # Druga liczba to ilosc probek
data.foreach(lambda x: print(x))
data.stats() # Wyświetla statystyki, ale każdy wiersz musi być intem
#KV
data.countByKey()
data.collectAsMap() # zwraca słwonik
data.lookup(key) # zwraca wartości dla zadanego klucza

#export do pliku
data.saveAsTextFile("katalog_wyjsciowy")


data.save()


data.count()

data.countByValue() # zlicza ilosc unikalnych wartosci dla jednorodnych rdd
data.countByKey() # zlicza ilosc unikalnych wartosci dla par k-v

data.mean() # dla liczbowych rdd
data.variance() # jw

#zapisanie RDD
data.saveAsTextFile("nazwa katalogu")

# cachowanie rdd
data.cache() #zapisanie tymmczasowych danych cw klastrze, żeby przy kazdym wywolaniu RDD nie parsowalo ich od nowa
from pyspark import StorageLevel
data.persist(StorageLevel.MEMORY_ONLY) # jw
data.unpersist() # usunięcie skaszowanych danych
"""
Poziom              Użyte miejsce   Czas CPU    W pamięci   Na dysku    Komentarze
MEMORY_ONLY         Dużo            Mało        T           N           
MEMORY_ONLY_SER     Mało            Dużo        T           N           Trochę czasu zajmuje serializacja, ale dane zajmują 2-5x mniej miejsca
MEMORY_AND_DISK     Dużo            Średnio     Częściowo   Częściowo   Jeśli danych jest zbyt dużo, aby zmieścić się w pamięci, wykorzystuje dysk.
MEMORY_AND_DISK_SER Mało            Dużo        Częściowo   Częściowo   Jeśli danych jest zbyt dużo, aby zmieściły się w pamięci, wykorzystuje dysk. Przechowuje dane w pamięci w postaci uszeregowanej.
DISK_ONLY           Mało            Dużo        N           T
"""

#combineBuKEy (liczenie sredniej):
nums = sc.parallelize([(1,1),(1,3),(1,5),(2,7)],3)
sumCount = nums.combineByKey((lambda x: (x,1)),
            (lambda x, y: (x[0] + y, x[1] + 1)),
            (lambda x, y: (x[0] + y[0], x[1] + y[1])))
r = sumCount.mapValues(lambda xy: xy[0]/xy[1]).collectAsMap()






data = sc.parallelize(data.take(10)) #lista cpollect zapisana ponownie jako RDD

data = data.groupBy(lambda x: x[3]).map(lambda x :(x[0], list(x[1]))) # x[3] staje się x[0], a calosc trafia do x[1]



# FUNCTIONS https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html



from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

data = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file_path")
data = spark.read.option("header", "true").option("inferSchema", "true")\
    .parquet("file_path.parquet")

data.write.save("plik.parquet")


# Operacje na DF
data.show(2) # pokazuje x rekordów
data.printSchema() # Pokazuje nagłówki i typy kolumn
data.filter(data["nazwa_kolumny"] == "wartosc kolumny") # Filtrowanie kolumn po określonej wartości
data.filter(data['nazwa_kolumny'].contains('wartość')) # Filtrowanie po wartościach stringów (odpowiednik in)
data.select(['kolumna1','kolumna2'])    # Wybranie tylko niektórych kolumn
data.groupBy("age").count().show() # grupowanie
data.groupBy("age").avg("friends").show()
data.rdd.map(lambda row: row) # DataFrame to RDD of rows
spark.createDataFrame(data).cache() # RDD of rows to DataFrame
data.select(data.name, data.age + 10) # zmiana kolumny o +10

data.createOrReplaceTempView('table_name')
data = spark.sql("SELECT * FROM table_name") # trowanie tymczasoej tablicy do przeszukiwania przez sql
# hiveCtx.cacheTable("tableName") # cacheowanie tablicy cache() tez powinno grac

data.withColumn('kolumna1', f.regexp_extract(f.col('kolumna1'), '(.+)....', 1)) # cztery ostatnie znaki z kazdej wartosci kolumny. regex dzieli stringa na 5 segmentów, wybiera pierwszy
data.withColumn('stara_nazwa', f.col('stara_nazwa').alias('nowa_nazwa')) #zmiana nazwy kolumny
data.withColumn('kolumna', f.col('kolumna').cast('float')) #zmiana typu kolumny
data.withColumn('kolumna',f.when(f.col('kolumna2') <= 5,f.col('kolumna')).otherwise(-f.col('kolumna'))) # zmiana wartości kolumny zależnie od wartości innej kolumny
