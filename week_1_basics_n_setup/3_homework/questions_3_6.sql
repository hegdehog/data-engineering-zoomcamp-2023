
/*
Question 3: Count records  (Multiple choice)
How many taxi trips were totally made on January 15?
*
*/
select count(1)
from green_taxi_trips
where date(lpep_pickup_datetime) = '2019-01-15' and date(lpep_dropoff_datetime) = '2019-01-15'


/*
Question 4: Largest trip for each day (Multiple choice)
Which was the day with the largest trip distance?
*/
select lpep_pickup_datetime, max(trip_distance)
from green_taxi_trips
group by lpep_pickup_datetime
order by 2 desc


/*
Question 5: The number of passengers  (Multiple choice)
In 2019-01-01 how many trips had 2 and 3 passengers?
*/
select passenger_count, count(1)
from green_taxi_trips
where date(lpep_pickup_datetime) = '2019-01-01'
group by passenger_count
order by 1


/*
Question 6: Largest tip (Multiple choice)
For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
*/

select tip_amount, B."zona", C."zona"
from green_taxi_trips  A
INNER JOIN zones  B on A."PULocationID" = B."LocationID"
INNER JOIN zones  C on A."DOLocationID" = C."LocationID"
where B."zona" = 'Astoria'
order by 1 desc