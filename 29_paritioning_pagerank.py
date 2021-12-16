from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf = conf)

links = ([1,(1,2)],[2,(1,2,3)],[3,(1)],[4,(1)])
ranks = (1,2,3,4)

links  = sc.parallelize(links).partitionBy(1).persist()
ranks = sc.parallelize(ranks).map(lambda x: (x, 1))

# joined = userData.join(events)

for i in range(0,10):
    # print(i)
    pass



contributions = links.join(ranks)


#     .flatMap {

#     case (pageId, (pageLinks, rank)) =>

#       pageLinks.map(dest => (dest, rank / pageLinks.size))

#   }

#   ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v =>

#   0.15 + 0.85*v)


# print(contributions)
# print(ranks.collect())
contributions.foreach(lambda x: print(x))
# ranks.foreach(lambda x: print(x))





# offTopicVisits = joined.filter(lambda row: row[1][1] not in row [1][0])

# offTopicVisits.foreach(lambda x: print(x))
# data = ([1,2],[4,4],[10,1])

# data = sc.parallelize(data)
# # data.foreach(lambda x: print(x))
# print(data.lookup(1))


sc.stop()