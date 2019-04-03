libname DataSet 'C:\Users\yliu10\Desktop\Group SQL Tableau\dataset';
/*Import the datasets*/
PROC IMPORT DATAFILE = 'C:\Users\yliu10\Desktop\Group SQL Tableau\dataset\airlines.csv' OUT = DataSet.Airlines REPLACE;
RUN;
PROC IMPORT DATAFILE = 'C:\Users\yliu10\Desktop\Group SQL Tableau\dataset\airports.csv' OUT = DataSet.Airports REPLACE;
RUN;
PROC IMPORT DATAFILE = 'C:\Users\yliu10\Desktop\Group SQL Tableau\dataset\flights.csv' OUT = DataSet.Flights REPLACE;
RUN;
PROC IMPORT DATAFILE = 'C:\Users\yliu10\Desktop\Group SQL Tableau\dataset\planes.csv' OUT = DataSet.Planes REPLACE;
RUN;
PROC IMPORT DATAFILE = 'C:\Users\yliu10\Desktop\Group SQL Tableau\dataset\weather.csv' OUT = DataSet.weather REPLACE;
RUN;

/*Initialize Data Sets to facilitate visualization of Airports Data*/
/*Include the Airlines data in Flights*/
PROC SQL;
CREATE TABLE Dataset.NewFlights AS
SELECT a.*,b.name
FROM Dataset.Flights a,Dataset.Airlines b
WHERE a.carrier = b.carrier;
QUIT;



/*Dest*/
PROC SQL;
CREATE TABLE Dest AS
SELECT A.arr_delay as delay, A.FlightKeyid, A.dest as airport
FROM Dataset.NewFlights A;
QUIT;
proc sql;
create table dest as
select *, case when length(A.airport) > 0 then 'Dest'
			end as origin_dest
from work.dest A;
quit;
/*Orign*/
PROC SQL;
CREATE TABLE  Origin as
SELECT A.dep_delay as delay, A.FlightKeyid, A.origin as airport
FROM Dataset.NewFlights A;
quit;
proc sql;
create table origin as
select *, case when length(A.airport) > 0 then 'origin'
			end as origin_dest
from work.origin A;
quit;
/*union*/
proc sql;
create table union as
select *
from work.Origin
union all
select *
from work.Dest;
quit;
/*add airport info to union*/
proc sql;
create table unionlocinfo as
select *
from work.union A
left join Dataset.Airports B
on A.Airport = B.faa;
quit;
/*extract from unionlocinfo as df1*/
proc sql;
create table dataset.df1 as
select DISTINCT A.airport, mean(A.delay) as avg_delay, A.lat, A.lon, A.Origin_dest
from work.unionlocinfo A
group by A.Airport;
quit;


/*basetable for df2 left join airports to flights*/
proc sql;
create table flights_d_airports as
select *
from dataset.flights A
left join dataset.airports B
on A.dest = B.faa;
quit;
/*CountFlight_AvgArrDelay_AvgdepDelay_DestAirport_OriginAirport_Distance as df2*/
proc sql;
create table dataset.df2 as
select A.Dest, A.origin,A.Distance, A.arr_delay,
A.dep_delay, A.flightkeyid as flight_id
from work.flights_d_airports A
group by A.Dest, A.Origin;
quit;




/*Add in Airline data in flights */
PROC SQL;
CREATE TABLE Dataset.Newplanes AS
SELECT *
FROM Dataset.NewFlights a LEFT OUTER JOIN Dataset.Airlines b
ON a.carrier = b.carrier;
QUIT;
/* Add in another column for status in the flights data with rehard to planes info*/
PROC SQL;
CREATE TABLE Dataset.Newplanes AS
SELECT *,
     CASE        WHEN dep_delay is null THEN "Cancelled"
                 WHEN dep_delay < 0 THEN "EarlyDep"
                 WHEN dep_delay > 0 THEN "DelayedDep"
	             WHEN dep_delay = 0 THEN "OntimeDep"
				  END as DepStatus ,

     CASE        WHEN arr_delay is null THEN "Cancelled"
                 WHEN arr_delay < 0 THEN "EarlyArriv"
                 WHEN arr_delay > 0 THEN "DelayedArriv"
	             WHEN arr_delay = 0 THEN "OntimeArriv"
				 END as ArrvStatus
FROM Dataset.Newplanes;
QUIT;
/* Prepare table to evaluate effect of Plane descriptors on delay */
PROC SQL;
CREATE TABLE Dataset.planesrelevant as
SELECT tailnum, year, manufacturer, model, seats
from Dataset.Planes
group by 1;
quit;
/*Join of new planesrelevant table with flights avg arrival and departure delays for both Cancelled flights and Actual flights*/
PROC SQL ;
CREATE TABLE dataset.planesandflights AS
SELECT a.*,b.name,b.AverageDept_Delay,b.AverageArr_Delay
from Dataset.planesrelevant a INNER JOIN
(
SELECT distinct origin,tailnum,name,time_hour,round(avg(dep_delay),1) as AverageDept_Delay,round(avg(arr_delay),1) AS AverageArr_Delay
FROM  Dataset.Newplanes
GROUP BY time_hour) as b
ON a.tailnum = b.tailnum
group by a.tailnum;
QUIT;
/* Weather */
PROC SQL;
CREATE TABLE Dataset.FlightsWeather AS
SELECT *
FROM Dataset.NewFlights a LEFT OUTER JOIN Dataset.Weather b
ON a.origin = b.origin
and a.time_hour = b.time_hour;
QUIT;
/* Add in another column for/*/
PROC SQL;
CREATE TABLE Dataset.NewWeather AS
SELECT *,
  CASE        WHEN dep_delay is null THEN "Cancelled"
                 WHEN dep_delay < 0 THEN "EarlyDep"
                 WHEN dep_delay > 0 THEN "DelayedDep"
	             WHEN dep_delay = 0 THEN "OntimeDep"
				  END as DepStatus ,

     CASE        WHEN arr_delay is null THEN "Cancelled"
                 WHEN arr_delay < 0 THEN "EarlyArriv"
                 WHEN arr_delay > 0 THEN "DelayedArriv"
	             WHEN arr_delay = 0 THEN "OntimeArriv"
				 END as ArrvStatus
FROM Dataset.FlightsWeather;
QUIT;

