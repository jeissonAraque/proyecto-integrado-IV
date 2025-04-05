-- TODO: Esta consulta devolverá una tabla con las diferencias entre los tiempos 
-- reales y estimados de entrega por mes y año. Tendrá varias columnas: 
-- month_no, con los números de mes del 01 al 12; month, con las primeras 3 letras 
-- de cada mes (ej. Ene, Feb); Year2016_real_time, con el tiempo promedio de 
-- entrega real por mes de 2016 (NaN si no existe); Year2017_real_time, con el 
-- tiempo promedio de entrega real por mes de 2017 (NaN si no existe); 
-- Year2018_real_time, con el tiempo promedio de entrega real por mes de 2018 
-- (NaN si no existe); Year2016_estimated_time, con el tiempo promedio estimado 
-- de entrega por mes de 2016 (NaN si no existe); Year2017_estimated_time, con 
-- el tiempo promedio estimado de entrega por mes de 2017 (NaN si no existe); y 
-- Year2018_estimated_time, con el tiempo promedio estimado de entrega por mes 
-- de 2018 (NaN si no existe).
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Considera tomar order_id distintos.
WITH monthly_data AS (
        SELECT
            SUBSTR(order_purchase_timestamp, 6, 2) AS month_no,
            CASE 
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '01' THEN 'Jan'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '02' THEN 'Feb'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '03' THEN 'Mar'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '04' THEN 'Apr'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '05' THEN 'May'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '06' THEN 'Jun'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '07' THEN 'Jul'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '08' THEN 'Aug'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '09' THEN 'Sep'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '10' THEN 'Oct'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '11' THEN 'Nov'
                WHEN SUBSTR(order_purchase_timestamp, 6, 2) = '12' THEN 'Dec'
            END AS month,
            SUBSTR(order_purchase_timestamp, 1, 4) AS year,
            JULIANDAY(order_delivered_customer_date) - JULIANDAY(order_purchase_timestamp) AS real_time,
            JULIANDAY(order_estimated_delivery_date) - JULIANDAY(order_purchase_timestamp) AS estimated_time
        FROM
            olist_orders
        WHERE
            order_status = 'delivered'
            AND order_delivered_customer_date IS NOT NULL
    )
    
    SELECT
        month_no,
        month,
        AVG(CASE WHEN year = '2016' THEN real_time ELSE NULL END) AS Year2016_real_time,
        AVG(CASE WHEN year = '2017' THEN real_time ELSE NULL END) AS Year2017_real_time,
        AVG(CASE WHEN year = '2018' THEN real_time ELSE NULL END) AS Year2018_real_time,
        AVG(CASE WHEN year = '2016' THEN estimated_time ELSE NULL END) AS Year2016_estimated_time,
        AVG(CASE WHEN year = '2017' THEN estimated_time ELSE NULL END) AS Year2017_estimated_time,
        AVG(CASE WHEN year = '2018' THEN estimated_time ELSE NULL END) AS Year2018_estimated_time
    FROM
        monthly_data
    GROUP BY
        month_no, month
    ORDER BY
        month_no
