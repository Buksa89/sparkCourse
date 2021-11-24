from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("flights")
sc = SparkContext(conf = conf)
input = sc.textFile("files/airplane_dataset/flights.csv")

def mapper(line):
    (year,month,day,day_of_week,airline,flight_number,tail_number,origin_airport,destination_airport,scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,air_time,distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled,cancellation_reason,air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay) = line.split(',')
    return None, (1, int(distance))


flights = input.map(mapper)
sum  = flights.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
avg = sum.mapValues(lambda x: x[1]/x[0])

print (avg.collect())
