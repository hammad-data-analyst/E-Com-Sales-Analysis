CREATE DATABASE SQL_PROJECT_3_ECOM;

USE SQL_PROJECT_3_ECOM;

SELECT * FROM [dbo].[olist_customers_dataset];
SELECT * FROM [dbo].[olist_geolocation_dataset];
SELECT * FROM [dbo].[olist_order_items_dataset];
SELECT * FROM [dbo].[olist_order_payments_dataset];
SELECT * FROM [dbo].[olist_order_reviews_dataset];
SELECT * FROM [dbo].[olist_orders_dataset];
SELECT * FROM [dbo].[olist_products_dataset];
SELECT * FROM [dbo].[olist_sellers_dataset];
SELECT * FROM [dbo].[product_category_name_translation];


-- A. TO BE EXPLORE
	--1. total sales inprocess/completed for each Year across the different states
	--1B. total sales unavailable/canceled for each Year across the different states
	
	--2. customer acquisitions for each Year across the different states
	--2B. customer loss for each Year across the different states
	
	--3. total no. of orders for each Year across the different states
	--3. total no. of orders return for each Year across the different states

	--    Does all the metrices show similar trends or is there any disparity amongst each of them?


	--b. Using the above metrics, identify the top 2 States which show
		--i. Declining NO OF ORDER'S trend over the years 
		--ii. Increasing NO OF ORDER'S trend over the years
--(Choose yourself the best suited metrics amognst all 3 in point (a) to carry out the analysis)


--1. total sales inprocess/completed for each Year across the different states
--1B. total sales unavailable/canceled for each Year across the different states

WITH TABLE1 AS
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
ROUND(SUM(C.price),2) EXPECTED_SALES
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp)),

TABLE2 AS
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
ROUND(SUM(C.price),2) UNAV_CANC_SALES
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE B.order_status IN ('unavailable','canceled')
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp)),

TABLE3 AS
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
ROUND(SUM(C.price),2) ACTUAL_STATE_SALES
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE B.order_status NOT IN ('unavailable','canceled')
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp))

SELECT TABLE1.*,TABLE2.UNAV_CANC_SALES,TABLE3.ACTUAL_STATE_SALES
FROM TABLE1 LEFT JOIN TABLE2 ON TABLE1.STATE_NAME=TABLE2.STATE_NAME
AND TABLE1.YEAR_=TABLE2.YEAR_
JOIN TABLE3 ON TABLE1.STATE_NAME=TABLE3.STATE_NAME
AND TABLE1.YEAR_=TABLE3.YEAR_
ORDER BY STATE_NAME
;

 

--2. customer acquisitions for each Year across the different states
--2B. customer loss for each Year across the different states

WITH TABLE1 AS
		(SELECT STATE_,YEAR(DATETIME_) YEAR_,COUNT(customer_unique_id) EXPECTED_CUSTOMERS_ACQUISITION
		FROM 
		
		(SELECT A.customer_state STATE_,A.customer_unique_id,CAST(B.order_purchase_timestamp AS DATE) DATETIME_,
	COUNT(B.order_id) OVER (PARTITION BY A.customer_unique_id) ORDERS,
	ROW_NUMBER() OVER (PARTITION BY A.customer_unique_id ORDER BY B.order_purchase_timestamp) RN
	FROM olist_customers_dataset A
	RIGHT JOIN olist_orders_dataset B ON A.customer_id=B.customer_id) X
		
		WHERE RN = 1
		GROUP BY STATE_,YEAR(DATETIME_)),


TABLE2 AS
	(SELECT STATE_,YEAR(DATETIME_) YEAR_,COUNT(customer_unique_id) DROPPED_CUSTOMERS_ACQUISITION
	FROM 

	(SELECT A.customer_state STATE_,A.customer_unique_id,CAST(B.order_purchase_timestamp AS DATE) DATETIME_,
	COUNT(B.order_id) OVER (PARTITION BY A.customer_unique_id) ORDERS,
	ROW_NUMBER() OVER (PARTITION BY A.customer_unique_id ORDER BY B.order_purchase_timestamp) RN
	FROM olist_customers_dataset A
	RIGHT JOIN olist_orders_dataset B ON A.customer_id=B.customer_id
	WHERE B.order_status IN ('unavailable','canceled')) Y

	WHERE RN = 1
	GROUP BY STATE_,YEAR(DATETIME_)),

TABLE3 AS
	(SELECT STATE_,YEAR(DATETIME_) YEAR_,COUNT(customer_unique_id) CONFIRMED_CUSTOMERS_ACQUISITION
	FROM 
	
	(SELECT A.customer_state STATE_,A.customer_unique_id,CAST(B.order_purchase_timestamp AS DATE) DATETIME_,
	COUNT(B.order_id) OVER (PARTITION BY A.customer_unique_id) ORDERS,
	ROW_NUMBER() OVER (PARTITION BY A.customer_unique_id ORDER BY B.order_purchase_timestamp) RN
	FROM olist_customers_dataset A
	RIGHT JOIN olist_orders_dataset B ON A.customer_id=B.customer_id
	WHERE B.order_status NOT IN ('unavailable','canceled')) Z

	WHERE RN = 1
	GROUP BY STATE_,YEAR(DATETIME_))

SELECT TABLE1.*,TABLE2.DROPPED_CUSTOMERS_ACQUISITION,TABLE3.CONFIRMED_CUSTOMERS_ACQUISITION
FROM TABLE1 LEFT JOIN TABLE2 ON TABLE1.STATE_=TABLE2.STATE_
AND TABLE1.YEAR_=TABLE2.YEAR_
JOIN TABLE3 ON TABLE1.STATE_=TABLE3.STATE_
AND TABLE1.YEAR_=TABLE3.YEAR_
ORDER BY STATE_
;


--3. total no. of orders for each Year across the different states
--3B. total no. of orders return for each Year across the different states

WITH TABLE1 AS
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
COUNT(C.order_id) EXPECTED_NO_ORDERS
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp)),

TABLE2 AS
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
COUNT(C.order_id) UNAV_CANC_NO_ORDERS
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE B.order_status IN ('unavailable','canceled')
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp)),

TABLE3 AS
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
COUNT(C.order_id) ACTUAL_NO_ORDERS
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE B.order_status NOT IN ('unavailable','canceled')
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp))

SELECT TABLE1.*,TABLE2.UNAV_CANC_NO_ORDERS,TABLE3.ACTUAL_NO_ORDERS
FROM TABLE1 LEFT JOIN TABLE2 ON TABLE1.STATE_NAME=TABLE2.STATE_NAME
AND TABLE1.YEAR_=TABLE2.YEAR_
JOIN TABLE3 ON TABLE1.STATE_NAME=TABLE3.STATE_NAME
AND TABLE1.YEAR_=TABLE3.YEAR_
ORDER BY STATE_NAME
;



--B. Using the above metrics, identify the top 2 States which show
		--i. Declining NO OF ORDER'S trend over the years 	
--(Choose yourself the best suited metrics amognst all 3 in point (a) to carry out the analysis)



WITH TABLE3 AS

(SELECT * FROM 
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
COUNT(C.order_id) ACTUAL_NO_ORDERS
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE B.order_status NOT IN ('unavailable','canceled')
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp)) X

PIVOT (SUM(ACTUAL_NO_ORDERS) FOR YEAR_ IN ([2016],[2017],[2018]))
PIVOT1)

SELECT TOP 2 *,[2017]-CASE WHEN [2016] IS NULL THEN 0 ELSE [2016] END AS DECLINE_ORDERS_OF_2017_FROM_2016
,[2018]-[2017] AS DECLINE_ORDERS_OF_2018_FROM_2017
FROM TABLE3
ORDER BY [2018]-[2017];



--B. Using the above metrics, identify the top 5 States which show
		--ii. Increasing NO OF ORDER'S trend over the years
--(Choose yourself the best suited metrics amognst all 3 in point (a) to carry out the analysis)

WITH TABLE3 AS

(SELECT * FROM 
(SELECT A.customer_state STATE_NAME,YEAR(B.order_purchase_timestamp) YEAR_,
COUNT(C.order_id) ACTUAL_NO_ORDERS
FROM olist_customers_dataset A JOIN
olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE B.order_status NOT IN ('unavailable','canceled')
GROUP BY A.customer_state,YEAR(B.order_purchase_timestamp)) X

PIVOT (SUM(ACTUAL_NO_ORDERS) FOR YEAR_ IN ([2016],[2017],[2018]))
PIVOT1)

SELECT TOP 2 * FROM
(SELECT *,[2017]-CASE WHEN [2016] IS NULL THEN 0 ELSE [2016] END AS INCREASE_ORDERS_OF_2017_FROM_2016
,[2018]-[2017] AS INCREASE_ORDERS_OF_2018_FROM_2017
FROM TABLE3) Y
ORDER BY INCREASE_ORDERS_OF_2017_FROM_2016+INCREASE_ORDERS_OF_2018_FROM_2017 DESC;



--c. For the States identified above, do the Root Cause analysis for their performance across a variety of metrics.
   --You can utilize the following metrics and explore a few yourself as well by analyzing the data.
		--Category level Sales and orders placed, post-order reviews, Seller performance in terms of deliveries, product-level sales & orders placed,
		--% of orders delivered earlier than the expected date, % of orders delivered later than the expected date, etc.


--Category level Sales YEARLY TREND

SELECT YEAR(B.order_purchase_timestamp) YEAR_,D.product_category_name,
CAST(SUM(C.price) AS DECIMAL(10,2)) TOTAL_SALES
FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
WHERE B.order_status NOT IN ('unavailable','canceled')
GROUP BY YEAR(B.order_purchase_timestamp),D.product_category_name
ORDER BY TOTAL_SALES DESC

--Category level orders placed YEARLY TREND

SELECT YEAR(B.order_purchase_timestamp) YEAR_,D.product_category_name,
COUNT(B.order_id) TOTAL_ORDERS
FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
WHERE B.order_status NOT IN ('unavailable','canceled')
GROUP BY YEAR(B.order_purchase_timestamp),D.product_category_name
ORDER BY TOTAL_ORDERS DESC



--TOP SELLING REVIEW Category level post-order reviews YEARLY TREND
SELECT YEAR(B.order_purchase_timestamp) YEAR_,D.product_category_name,
AVG(E.review_score) AVG_REVIEW
FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
JOIN olist_order_reviews_dataset E ON B.order_id=E.order_id
WHERE B.order_status NOT IN ('unavailable','canceled')

AND D.product_category_name IN
('cama_mesa_banho',
'beleza_saude',
'esporte_lazer',
'utilidades_domesticas',
'moveis_decoracao',
'informatica_acessorios',
'cama_mesa_banho',
'relogios_presentes',
'brinquedos',
'telefonia')


GROUP BY YEAR(B.order_purchase_timestamp),D.product_category_name
ORDER BY AVG_REVIEW DESC


--LEAST SELLING REVIEW Category level post-order reviews YEARLY TREND
SELECT YEAR(B.order_purchase_timestamp) YEAR_,D.product_category_name,
AVG(E.review_score) AVG_REVIEW
FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
JOIN olist_order_reviews_dataset E ON B.order_id=E.order_id
WHERE B.order_status NOT IN ('unavailable','canceled')

AND D.product_category_name IN
('instrumentos_musicais',
'informatica_acessorios',
'eletronicos',
'automotivo'
)


GROUP BY YEAR(B.order_purchase_timestamp),D.product_category_name
ORDER BY AVG_REVIEW DESC


--STATE WISE % of orders delivered earlier than the expected date

WITH TABLE1 AS
(SELECT A.customer_state,B.order_id,CAST(B.order_estimated_delivery_date AS DATE) estimated_delivery_date,
CAST(B.order_delivered_customer_date AS DATE) delivered_date
FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
WHERE B.order_status IN ('delivered')),

TABLE2 AS
(SELECT YEAR(delivered_date) YEAR_,customer_state,COUNT(order_id) TOTAL_DELIVERIES FROM TABLE1
GROUP BY YEAR(delivered_date),customer_state),

TABLE3 AS
(SELECT YEAR(delivered_date) YEAR_,customer_state,COUNT(order_id) BEORE_DATE_DELIVERIES FROM TABLE1
WHERE delivered_date <= estimated_delivery_date
GROUP BY YEAR(delivered_date),customer_state )

SELECT TABLE2.YEAR_,TABLE2.customer_state,TOTAL_DELIVERIES,BEORE_DATE_DELIVERIES
FROM TABLE2 JOIN TABLE3 ON TABLE2.customer_state=TABLE3.customer_state
WHERE TABLE2.customer_state IN ('RJ','SP','SE','RO')
AND TABLE2.YEAR_ IS NOT NULL;



--STATE WISE % of orders delivered LATER than the expected date

WITH TABLE1 AS
(SELECT A.customer_state,B.order_id,CAST(B.order_estimated_delivery_date AS DATE) estimated_delivery_date,
CAST(B.order_delivered_customer_date AS DATE) delivered_date
FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
WHERE B.order_status IN ('delivered')),

TABLE2 AS
(SELECT YEAR(delivered_date) YEAR_,customer_state,COUNT(order_id) TOTAL_DELIVERIES FROM TABLE1
GROUP BY YEAR(delivered_date),customer_state),

TABLE3 AS
(SELECT YEAR(delivered_date) YEAR_,customer_state,COUNT(order_id) AFTER_DATE_DELIVERIES FROM TABLE1
WHERE delivered_date > estimated_delivery_date
GROUP BY YEAR(delivered_date),customer_state )

SELECT TABLE2.YEAR_,TABLE2.customer_state,TOTAL_DELIVERIES,AFTER_DATE_DELIVERIES
FROM TABLE2 JOIN TABLE3 ON TABLE2.customer_state=TABLE3.customer_state










--d. Do the above analysis for the top 2 cities which are causing the 
--trend for each of the states identified in point (b)



--FINDIN THE Top 2 Decreasing Trend CITY OF STATE RO

SELECT * FROM
(SELECT A.customer_city,COUNT(B.order_id) ORDERS,SUM(C.price) SALES
,DENSE_RANK() OVER (ORDER BY COUNT(B.order_id),SUM(C.price)) RK
FROM olist_customers_dataset A
JOIN olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE A.customer_state IN ('RO')
GROUP BY A.customer_city) X
WHERE RK IN (1,2)

--FINDIN THE Top 2 Decreasing Trend CITY OF STATE SE

SELECT * FROM
(SELECT A.customer_city,COUNT(B.order_id) ORDERS,SUM(C.price) SALES
,DENSE_RANK() OVER (ORDER BY COUNT(B.order_id),SUM(C.price)) RK
FROM olist_customers_dataset A
JOIN olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE A.customer_state IN ('SE')
GROUP BY A.customer_city) X
WHERE RK IN (1,2)







--Top 2 CITIES OF Increasing Trend STATES SP

SELECT * FROM
(SELECT A.customer_city,COUNT(B.order_id) ORDERS,SUM(C.price) SALES,
DENSE_RANK() OVER (ORDER BY COUNT(B.order_id) DESC) RK
FROM olist_customers_dataset A
JOIN olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE A.customer_state IN ('SP')
GROUP BY A.customer_city) X
WHERE RK IN (1,2)

--Top 2 CITIES OF Increasing Trend STATES RJ

SELECT * FROM
(SELECT A.customer_city,COUNT(B.order_id) ORDERS,SUM(C.price) SALES,
DENSE_RANK() OVER (ORDER BY COUNT(B.order_id) DESC) RK
FROM olist_customers_dataset A
JOIN olist_orders_dataset B ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
WHERE A.customer_state IN ('RJ')
GROUP BY A.customer_city) X
WHERE RK IN (1,2)



--INCREASING TREND CITY OF STATE RJ & SP

SELECT YEAR(B.order_purchase_timestamp) YEAR_,A.customer_state,A.customer_city,

COUNT(B.order_id) ORDERS,
CAST(SUM(C.price) AS DECIMAL(10,2)) SALES

FROM olist_customers_dataset A JOIN olist_orders_dataset B ON 
A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
JOIN olist_order_reviews_dataset E ON B.order_id=E.order_id

WHERE B.order_status NOT IN ('unavailable','canceled')
AND A.customer_city IN ('rio de janeiro','niteroi','sao paulo','campinas')

GROUP BY YEAR(B.order_purchase_timestamp),A.customer_state,A.customer_city




--DECREASING TREND CITY OF STATE RJ & SP

SELECT YEAR(B.order_purchase_timestamp) YEAR_,A.customer_state,A.customer_city,

COUNT(B.order_id) ORDERS,
CAST(SUM(C.price) AS DECIMAL(10,2)) SALES

FROM olist_customers_dataset A JOIN olist_orders_dataset B ON 
A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
JOIN olist_order_reviews_dataset E ON B.order_id=E.order_id

WHERE B.order_status NOT IN ('unavailable','canceled')
AND A.customer_city IN ('santo amaro das brotas','sao miguel do aleixo',
'mutum parana','castanheiras')

GROUP BY YEAR(B.order_purchase_timestamp),A.customer_state,A.customer_city



--BEST PERFORMED Product Categories Under Best Performed Cities
SELECT TOP 10 A.customer_city,D.product_category_name,COUNT(B.order_id) NO_OF_ORDERS FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
WHERE A.customer_city IN ('rio de janeiro','niteroi','sao paulo','campinas')
GROUP BY A.customer_city,D.product_category_name
ORDER BY NO_OF_ORDERS DESC;


--BEST PERFORMED Product Categories All Cities
SELECT TOP 10 A.customer_city,D.product_category_name,COUNT(B.order_id) NO_OF_ORDERS FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
GROUP BY A.customer_city,D.product_category_name
ORDER BY NO_OF_ORDERS DESC;



--WROST PERFORMED Product Categories Under WORST Performed Cities
SELECT TOP 10 A.customer_city,D.product_category_name,COUNT(B.order_id) NO_OF_ORDERS FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
WHERE A.customer_city IN ('castanheiras','mutum parana','santo amaro das brotas','sao miguel do aleixo')
GROUP BY A.customer_city,D.product_category_name
ORDER BY NO_OF_ORDERS;


--WROST PERFORMED Product Categories Under ALL Cities

SELECT DISTINCT * FROM 
(SELECT A.customer_city,D.product_category_name,COUNT(B.order_id) NO_OF_ORDERS FROM olist_customers_dataset A JOIN olist_orders_dataset B
ON A.customer_id=B.customer_id
JOIN olist_order_items_dataset C ON B.order_id=C.order_id
JOIN olist_products_dataset D ON C.product_id=D.product_id
GROUP BY A.customer_city,D.product_category_name) X
WHERE NO_OF_ORDERS=1