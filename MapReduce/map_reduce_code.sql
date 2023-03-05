show databases;

use default;

CREATE EXTERNAL TABLE IF NOT EXISTS flight_data(
  Counter int,
  Year int, 
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled string,
  CancellationCode string,
  Diverted string,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION '/user/tables/flight_data';

show tables;

LOAD DATA INPATH 's3://sparkvsmapreducesrw/dataset/DelayedFlights-updated.csv' OVERWRITE INTO TABLE flight_data;

	
SET hive.cli.print.header=true;

--Carrier Delay Query
SELECT Year As Year, AVG((CarrierDelay/ArrDelay)*100) As AverageCarrierDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

--NAS Delay Query
SELECT Year As Year, AVG((NASDelay/ArrDelay)*100) AS AverageNASDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

--Weather Delay Query
SELECT Year As Year, AVG((WeatherDelay/ArrDelay)*100) AS AverageWeatherDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

--Late Aircraft Delay Query
SELECT Year As Year, AVG((LateAircraftDelay/ArrDelay)*100) AS AverageLateAircraftDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

--Security Delay Query
SELECT Year As Year, AVG((SecurityDelay/ArrDelay)*100) AS AverageSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

quit;