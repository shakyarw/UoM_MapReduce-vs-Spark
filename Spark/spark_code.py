from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("FlightData").getOrCreate()
flight_data = spark.read.format("csv").option("header", "true").load("s3://sparkvsmapreducesrw/dataset/DelayedFlights-updated.csv")
flight_data.createOrReplaceTempView("flight_data")


#Carrier Delay Query
start_t = time.time()
spark.sql("""
	SELECT Year As Year, AVG((CarrierDelay/ArrDelay)*100) As AverageCarrierDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_t = time.time()
run_t = end_t - start_t
print("Time spent for Carrier Delay: ", run_t, " s")


#NAS Delay Query
start_t = time.time()
spark.sql("""
	SELECT Year As Year, AVG((NASDelay/ArrDelay)*100) As AverageNASDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_t = time.time()
run_t = end_t - start_t
print("Time spent for NAS Delay: ", run_t, " s")


#Weather Delay Query
start_t = time.time()
spark.sql("""
	SELECT Year As Year, AVG((WeatherDelay/ArrDelay)*100) As AverageWeatherDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_t = time.time()
run_t = end_t - start_t
print("Time spent for Weather Delay: ", run_t, " s")


#Late Aircraft Delay Query
start_t = time.time()
spark.sql("""
	SELECT Year As Year, AVG((LateAircraftDelay/ArrDelay)*100) As AverageLateAircraftDelay
    FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_t = time.time()
run_t = end_t - start_t
print("Time spent for Late Aircraft Delay: ", run_t, " s")

#Security Delay Query
start_t = time.time()
spark.sql("""
	SELECT Year As Year, AVG((SecurityDelay/ArrDelay)*100) As AverageSecurityDelay
    FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_t = time.time()
run_t = end_t - start_t
print("Time spent for Security Delay: ", run_t, " s")