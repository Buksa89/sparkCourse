from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("DegreesOfSeparation").getOrCreate()
sc = spark.sparkContext

startCharacterID = 12 #ABOMINATRIX
targetCharacterID = 14  #ADAM 3,031 (who?)

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("files/Marvel-names.txt")
hero_connections = sc.textFile("files/Marvel-graph.txt")



end = sc.accumulator(0)

def find_name(id):
    return names.filter(f.col("id") == id).select("name").first()['name']

def connections_mapper(line):
    line = line.strip().split()
    heroID = int(line[0])
    connections = list(map(int, line[1:]))
    return heroID, connections

def connections_map_DES_parameters(row):
    heroID = row[0]
    connections = list(set(row[1]))
    way = []
    color = 1 if heroID==startCharacterID else 0
    return heroID, [connections, way, color]

def next_step(row):
    results = []
    heroID = row[0]
    data = row[1]
    connections = data[0]
    way = data[1]
    color = data[2]
    if color == 1:
        color = 2
        for new_hero in connections:
            if (targetCharacterID == new_hero):
                end.add(1)
            results.append([new_hero, [[], way+[heroID], 1]])
    results.append([heroID, [connections, way, color]])
    return results

def update_heroes(clone_1, clone_2):
    [connections_1, way_1, color_1] = clone_1
    [connections_2, way_2, color_2] = clone_2

    if color_1 == 2: return clone_1
    elif color_2 == 2: return clone_2
    elif color_1 == 1:
        connections = connections_2
        way = way_1
        color = 1
    elif color_2 == 1:
        connections = connections_1
        way = way_2
        color = 1
    return connections, way, color

def names_mapper(line):
    line = line.split(',')
    return line[0], line[1]

hero_connections = hero_connections.map(connections_mapper)
hero_connections = hero_connections.reduceByKey(lambda x, y: x + y)
hero_connections = hero_connections.map(connections_map_DES_parameters)

counter = 1

while not end.value and counter != 10:
    
    print(counter)
    hero_connections = hero_connections.flatMap(next_step)
    hero_connections = hero_connections.reduceByKey(update_heroes)

    hero_connections.count()
    counter+=1

hero_connections = hero_connections.filter(lambda x: x[0]==targetCharacterID)
hero = hero_connections.collect()

print('---')
way = list(map(find_name, hero[0][1][1]))
print(way[0])
for i in way[1:]: print(i)
print(find_name(14))


spark.stop()