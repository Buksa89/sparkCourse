from pyspark import SparkConf, SparkContext

## Zasada joinowania
# x = sc.parallelize([("a", 1), ("b", 4)])
# y = sc.parallelize([("a", 2), ("a", 3)])
# sorted(x.join(y).collect())
# [('a', (1, 2)), ('a', (1, 3))]


conf = SparkConf().setMaster("local").setAppName("flights")
sc = SparkContext(conf = conf)
input = sc.textFile("files/airplane_dataset/flights.csv")
names = sc.textFile("files/airplane_dataset/airlines.csv")

def flights_mapper(line):
    (year,month,day,day_of_week,airline,flight_number,tail_number,origin_airport,destination_airport,scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled,cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay) = line.split(',')
    departure_delay = int(departure_delay) if departure_delay else 0
    arrival_delay = int(arrival_delay) if arrival_delay else 0
    month = int(month) if month else 0
    return airline, (1, departure_delay, arrival_delay)

def names_mapper(line):
    line = line.split(',')
    return line[0], line[1]

def join_mapper(line):
    name = line[1][0]
    data = line[1][1]
    return name,data

names = names.map(names_mapper)

flights = input.map(flights_mapper)


sum  = flights.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
avg = sum.mapValues(lambda x: (x[1]/x[0],x[2]/x[0]))


final = names.join(avg).map(join_mapper)


for key, value in final.collect():
    print (f'{key}\t{value}')