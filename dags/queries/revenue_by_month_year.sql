-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).

WITH RECURSIVE months_numbers AS (
    SELECT '01' AS month_no, 'Jan' AS month
    UNION ALL
    SELECT 
        PRINTF('%02d', CAST(month_no AS INTEGER) + 1),
        CASE CAST(month_no AS INTEGER) + 1
            WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr'
            WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul'
            WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct'
            WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
        END
    FROM months_numbers
    WHERE month_no < '12'
),
monthly_payments AS (
    SELECT
        o.order_id,
        strftime('%Y', o.order_delivered_customer_date) AS delivery_year,
        strftime('%m', o.order_delivered_customer_date) AS delivery_month,
        p.payment_value AS total_payment
    FROM olist_orders o
    JOIN olist_order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = "delivered"
    AND o.order_delivered_customer_date IS NOT NULL
    GROUP BY o.order_id, delivery_year, delivery_month
)

SELECT
    m.month_no,
    m.month,
    COALESCE((SELECT SUM(total_payment) FROM monthly_payments 
              WHERE delivery_year = '2016' AND delivery_month = m.month_no), 0.0) AS Year2016,
    COALESCE((SELECT SUM(total_payment) FROM monthly_payments 
              WHERE delivery_year = '2017' AND delivery_month = m.month_no), 0.0) AS Year2017,
    COALESCE((SELECT SUM(total_payment) FROM monthly_payments 
              WHERE delivery_year = '2018' AND delivery_month = m.month_no), 0.0) AS Year2018
FROM months_numbers m
ORDER BY m.month_no;




