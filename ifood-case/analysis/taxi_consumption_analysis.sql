-- Q1: Average total amount per month (yellow taxis)
WITH months_order AS  (
SELECT DATE_FORMAT(pep_pickup_datetime, 'MMMM') month_name
      ,MONTH(pep_pickup_datetime) month_order
      ,CAST(AVG(total_amount) AS DECIMAL(10,2)) avg_total_amount_usd
  FROM datalake_gold.taxi_consumption
 WHERE 1=1
   AND taxi_type = 'yellow'
 GROUP BY month_name, month_order)

SELECT month_name
      ,avg_total_amount_usd
  FROM months_order
 ORDER BY month_order

-- Q2: Average passengers per hour in May (all taxis)
SELECT CONCAT(LPAD(HOUR(pep_pickup_datetime), 2, '0'), ':00') hour_of_day
      ,CAST(ROUND(AVG(passenger_count), 2) AS DECIMAL(10,2)) avg_passengers
  FROM datalake_gold.taxi_consumption
 WHERE 1=1
   AND MONTH(pep_pickup_datetime) = 5
 GROUP BY hour_of_day
 ORDER BY hour_of_day