-- Create Inventory table to replicate Agustin's Inventory Evolution table

-- Create inventory table who only has update records
DROP TABLE IF EXISTS inventory;
CREATE TEMP TABLE inventory AS

SELECT a.updated_at AS inventory_update_utc_timestamp
     , DATEADD('HOUR', SUBSTRING(a.updated_at,20,3)::integer, LEFT(a.updated_at,19)::timestamp) AS inventory_update_local_timestamp
     , c.name AS inventory_location
     , a.location_id
     , d.id AS variant_id
     , e.product_type
     , e.title AS product_title
     , a.available AS inventory_quantity
     , a.current_indicator AS current_inventory_indicator
FROM stockup_mart_vw.inventory_levels a
LEFT JOIN stockup_mart_vw.products__variants d ON a.inventory_item_id = d.inventory_item_id
LEFT JOIN stockup_mart_vw.products e ON d._sdc_source_key_id = e.id
LEFT JOIN stockup_mart_vw.locations c ON a.location_id = c.id
WHERE NOT (inventory_location = 'MIA-003' AND variant_id = 34047167496332 AND inventory_update_utc_timestamp = '2020-11-15T10:20:45-05:00') --remove incorrect inventory input
;

-- convert only-update into start-end-date inventory level
DROP TABLE IF EXISTS start_end_date_inventory;
CREATE TEMP TABLE start_end_date_inventory AS

SELECT inventory_location
     , location_id
     , inventory_update_local_timestamp
     , variant_id
     , product_type
     , product_title
     , inventory_quantity
     , start_timestamp
     , CASE WHEN end_timestamp ::timestamp IS NULL THEN DATEADD('HOUR', SUBSTRING(inventory_update_utc_timestamp,20,3)::integer, sysdate) ELSE end_timestamp ::timestamp END AS end_timestamp
     , start_timestamp ::date AS start_date
     , CASE WHEN end_timestamp ::timestamp IS NULL THEN DATEADD('HOUR', SUBSTRING(inventory_update_utc_timestamp,20,3)::integer, sysdate) ::date ELSE DATEADD('DAY',-1, end_timestamp ::date) ::date END AS end_date
     , current_inventory_indicator
FROM (
     SELECT inventory_location
          , location_id
          , inventory_update_utc_timestamp
          , inventory_update_local_timestamp
          , variant_id
          , product_type
          , product_title
          , inventory_quantity
          , inventory_update_local_timestamp AS start_timestamp
          , LAG(inventory_update_local_timestamp) OVER (PARTITION BY inventory_location, variant_id ORDER BY inventory_update_local_timestamp DESC) AS end_timestamp
          , current_inventory_indicator
    FROM inventory
    )
;

-- only keep the most recent record per day for each variant in each location
DROP TABLE IF EXISTS start_end_date_inventory_dedup;
CREATE TEMP TABLE start_end_date_inventory_dedup AS

SELECT DISTINCT *
FROM start_end_date_inventory
WHERE (inventory_location, variant_id, start_timestamp) IN (
    SELECT inventory_location
         , variant_id
         , MAX(start_timestamp) AS start_timestamp
    FROM start_end_date_inventory
    GROUP BY 1,2,start_date
    )
AND inventory_location <> 'MIA-027 Warehouse Receiving'
AND inventory_location <> 'GVA-Training'
;


-- Create a date table includes today
DROP TABLE IF EXISTS reference_dates;

CREATE TEMP TABLE reference_dates AS

SELECT DATEADD(DAY, 2, date) ::date AS date
     , DATE_PART(dow, date) ::integer AS day_of_week_num
     , CASE
         WHEN day_of_week_num = 0 THEN 'Tuesday'
         WHEN day_of_week_num = 1 THEN 'Wednesday'
         WHEN day_of_week_num = 2 THEN 'Thursday'
         WHEN day_of_week_num = 3 THEN 'Friday'
         WHEN day_of_week_num = 4 THEN 'Saturday'
         WHEN day_of_week_num = 5 THEN 'Sunday'
         WHEN day_of_week_num = 6 THEN 'Monday'
         END ::text AS day_of_week
FROM reference.dates
WHERE date >= '2020-09-07'
;


-- Convert start-end-date level into daily inventory level
DROP TABLE IF EXISTS daily_inventory;

CREATE TEMP TABLE daily_inventory AS

SELECT b.date
     , b.day_of_week
     , a.inventory_location
     , CASE
         WHEN SUBSTRING(a.inventory_location,1,3)::text = 'MIA' THEN 'Miami'
         WHEN a.inventory_location IS NULL THEN NULL
         ELSE 'New MSA' END ::text AS msa
     , a.location_id
     , a.variant_id
     , a.product_type
     , a.product_title
     , a.inventory_quantity
     , a.current_inventory_indicator
FROM start_end_date_inventory_dedup a
JOIN reference_dates b ON b.date BETWEEN a.start_date AND a.end_date
;


-- Create Product table
DROP TABLE IF EXISTS product;

CREATE TEMP TABLE product AS

SELECT DATEADD('HOUR', -4, load_date) AS load_timestamp
     , load_date ::date AS date
     , variant_id_shopify
     , product_name
     , CASE WHEN macro_category = '0' THEN NULL ELSE macro_category END AS macro_category
     , CASE WHEN micro_category = '0' THEN NULL ELSE micro_category END AS micro_category
     , LOWER(target_selection) AS target_selection
     , supplier_status
     , unit_cost
FROM grocery.product
WHERE variant_id_shopify <> 1
AND variant_id_shopify IS NOT NULL
AND (load_date) IN (
    SELECT MAX(load_date)
    FROM grocery.product
    GROUP BY load_date ::date
    )
;

-- ***Create product_inventory table
DROP TABLE IF EXISTS scratch.product_inventory;

CREATE TABLE scratch.product_inventory AS

SELECT a.date
     , b.inventory_location AS location_code
     , a.variant_id_shopify AS variant_id
     , a.product_name AS product_title
     , a.macro_category
     , a.micro_category
     , a.target_selection
     , a.supplier_status
     , b.inventory_quantity
FROM product a
LEFT JOIN (
    SELECT date, variant_id, inventory_location, SUM(inventory_quantity) AS inventory_quantity
    FROM daily_inventory
    GROUP BY 1,2,3
    ) b
ON a.date = b.date AND a.variant_id_shopify = b.variant_id
;


-- Create Sales Table (To replicate Agustin's Actual Sales table)
-- Create a daily-location-order-variant level sales table

--DROP TABLE IF EXISTS discount;

--CREATE TEMP TABLE discount AS

--SELECT _sdc_source_key_id
--     , _sdc_level_0_id
--    , SUM(amount_set__shop_money__amount) AS amount_set__shop_money__amount
--FROM stockup_mart_vw.orders__line_items__discount_allocations
--GROUP BY 1,2
--;


DROP TABLE IF EXISTS sales;

CREATE TEMP TABLE sales AS

SELECT (a.customer__first_name || ' ' || a.customer__last_name) AS customer_name
     , CASE
            WHEN customer_name IN ('DP 014', 'DP Sales 014') THEN 45648871564
            WHEN customer_name = 'DP Sales 017' THEN 45674758284
            WHEN customer_name = 'DP Sales 01' THEN 43395416204
            ELSE a.location_id
            END ::bigint AS sales_location_id
     , DATEADD('HOUR', -4, a.created_at ::timestamp) AS local_order_timestamp
     , local_order_timestamp ::date AS local_order_date
     , a.id AS order_id
     , b.variant_id
     --, COALESCE(i.product_id_shopify, b.product_id) AS product_id
     , COALESCE(i.product_name, b.title) AS product_title
     , i.target_selection
     , i.macro_category
     , i.micro_category
     --, b.vendor
     --, b.fulfillment_status
     , CASE WHEN a.cancelled_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_cancelled
     --, b.product_exists
     , b.price_set__shop_money__amount ::float AS sku_price
     , b.quantity ::integer AS sales_quantity
     --, TRUNC( (sku_price ::float * sales_quantity) ::float , 2) AS gross_sales
     --, CASE WHEN c.amount_set__shop_money__amount IS NULL THEN 0 ELSE (c.amount_set__shop_money__amount ::float) END AS discounts --discount considered item quantity
     --, CASE WHEN (d.subtotal ::float) IS NULL THEN 0 ELSE (d.subtotal ::float) END AS refunds
     , CASE WHEN (d.quantity ::numeric) IS NULL THEN 0 ELSE (d.quantity ::numeric) END AS refund_quantity
     --, TRUNC( (gross_sales - discounts - refunds) ::float, 2) AS net_sales
     , i.unit_cost ::float AS unit_cost
     --, CASE WHEN (sales_quantity - refund_quantity) = 0 THEN 0 ELSE TRUNC( (unit_cost ::float * (sales_quantity - refund_quantity) ), 2) ::float END AS total_cost
     --, TRUNC( (net_sales - total_cost) ::float, 2) AS gross_margin
     --, row_number() over (partition by b._sdc_source_key_id, b._sdc_level_0_id ) as rn
FROM      stockup_mart_vw.orders a
LEFT JOIN stockup_mart_vw.orders__line_items b ON a.id = b._sdc_source_key_id
--LEFT JOIN discount c ON b._sdc_source_key_id = c._sdc_source_key_id AND b._sdc_level_0_id = c._sdc_level_0_id
LEFT JOIN stockup_mart_vw.orders__refunds__refund_line_items d ON b.id = d.line_item_id
LEFT JOIN product i ON a.created_at ::date = i.date AND b.variant_id = i.variant_id_shopify
WHERE local_order_date >= '2020-09-09'
AND is_cancelled = FALSE
AND b.variant_id IS NOT NULL
;


-- Only keep daily-location-variant level sales, no order level

DROP TABLE IF EXISTS sales_dedup;

CREATE TEMP TABLE sales_dedup AS

WITH agg AS(
    SELECT a.sales_location_id
         , a.local_order_date
         , a.variant_id
         , SUM(a.sales_quantity) AS sales_quantity
         , SUM(a.refund_quantity) AS refund_quantity
FROM sales a
GROUP BY 1,2,3
)

SELECT a.*
     , b.sku_price
     , b.customer_name
FROM agg a
LEFT JOIN sales b ON a.sales_location_id = b.sales_location_id AND a.local_order_date = b.local_order_date AND a.variant_id = b.variant_id
;

--select count(*) from sales_dedup; --10283
--select count(*) from sales_dedup where target_selection is null; --193


-- ***Create the hourly sales table

DROP TABLE IF EXISTS scratch.hourly_variant_sales;

CREATE TABLE scratch.hourly_variant_sales AS

SELECT a.sales_location_id
     , b.name AS location_code
     , CASE
         WHEN SUBSTRING(location_code,1,3)::text = 'MIA' THEN 'Miami'
         WHEN location_code IS NULL THEN NULL
         ELSE 'New MSA' END ::text AS msa
     , a.local_order_timestamp
     , a.variant_id
     , a.product_title
     , a.target_selection
     , a.macro_category
     , a.micro_category
     , a.sku_price
     , SUM(a.sales_quantity) AS sales_quantity
     , SUM(a.refund_quantity) AS refund_quantity
     , SUM(a.sales_quantity - a.refund_quantity) AS net_sales_quantity
FROM sales a
LEFT JOIN stockup_mart_vw.locations b ON a.sales_location_id = b.id
GROUP BY 1,2,3,4,5,6,7,8,9,10
;


DROP TABLE IF EXISTS vessel_sales;

CREATE TEMP TABLE vessel_sales AS

SELECT * FROM sales_dedup
WHERE sales_location_id IS NOT NULL
;


DROP TABLE IF EXISTS vessel_sales_null;

CREATE TEMP TABLE vessel_sales_null AS

SELECT * FROM sales_dedup
WHERE sales_location_id IS NULL
;


DROP TABLE IF EXISTS reference_dates_2;

CREATE TEMP TABLE reference_dates_2 AS

SELECT DISTINCT a.date
              , b.variant_id
FROM reference_dates a
LEFT JOIN (
    SELECT local_order_date
         , variant_id
    FROM vessel_sales_null
    ) b
ON b.local_order_date BETWEEN (SELECT MIN(date) FROM reference_dates) AND (SELECT MAX(date) FROM reference_dates)
--order by 2,1 asc
;


-- Join sales and daily inventory into one master table
-- 01: Union location and null location sales together

DROP TABLE IF EXISTS daily_inventory_sales_1;

CREATE TEMP TABLE daily_inventory_sales_1 AS

WITH join_table_1 AS (
    SELECT a.date
         , DATEADD('DAY', -7, a.date) ::date AS last_7_days_begin
         , DATEADD('DAY', -3, a.date) ::date AS last_3_days_begin
         , DATEADD('DAY', -28, a.date) ::date AS last_28_days_begin
         , a.day_of_week
         , a.inventory_location AS location_code
         , a.location_id
         , a.msa
         , a.variant_id
         , a.inventory_quantity
         , a.current_inventory_indicator
         , b.sku_price
         , b.customer_name
         , CASE WHEN b.sales_quantity IS NULL THEN 0 ELSE b.sales_quantity END AS sku_sales_quantity
         , CASE WHEN b.refund_quantity IS NULL THEN 0 ELSE b.refund_quantity END AS sku_refund_quantity
    FROM daily_inventory a
    LEFT JOIN vessel_sales b ON a.date = b.local_order_date AND a.location_id = b.sales_location_id AND a.variant_id = b.variant_id

    UNION

    SELECT c.date AS date
         , DATEADD('DAY', -7, c.date) ::date AS last_7_days_begin
         , DATEADD('DAY', -3, c.date) ::date AS last_3_days_begin
         , DATEADD('DAY', -28, c.date) ::date AS last_28_days_begin
         , CASE
             WHEN DATE_PART(dow, date) = 0 THEN 'Sunday'
             WHEN DATE_PART(dow, date) = 1 THEN 'Monday'
             WHEN DATE_PART(dow, date) = 2 THEN 'Tuesday'
             WHEN DATE_PART(dow, date) = 3 THEN 'Wednesday'
             WHEN DATE_PART(dow, date) = 4 THEN 'Thursday'
             WHEN DATE_PART(dow, date) = 5 THEN 'Friday'
             WHEN DATE_PART(dow, date) = 6 THEN 'Saturday'
         END ::text AS day_of_week
         , 'MIA-blanks' ::text AS location_code
         , 00000000000 ::bigint AS location_id
         , 'Miami'AS msa
         , c.variant_id
         , NULL AS inventory_quantity
         , NULL AS current_inventory_indicator
         , d.sku_price
         , d.customer_name
         , CASE WHEN d.sales_quantity IS NULL THEN 0 ELSE d.sales_quantity END AS sku_sales_quantity
         , CASE WHEN d.refund_quantity IS NULL THEN 0 ELSE d.refund_quantity END AS sku_refund_quantity
    FROM reference_dates_2 c
    LEFT JOIN vessel_sales_null d ON c.date = d.local_order_date AND c.variant_id = d.variant_id
    --order by 7,4,1 asc
)

SELECT j.*
     , p.product_name AS product_title
     , p.target_selection
     , p.macro_category
     , p.micro_category
     , p.unit_cost
FROM join_table_1 j
LEFT JOIN product p ON j.date = p.date AND j.variant_id = p.variant_id_shopify
;


-- 02: To calculate last 7 days sales on variant-location level

DROP TABLE IF EXISTS daily_inventory_sales_2;

CREATE TEMP TABLE daily_inventory_sales_2 AS

WITH join_table_2 AS (
SELECT dis1.date
     , dis1.location_id
     , dis1.variant_id
     , CASE
         WHEN dis1.date < '2020-09-16' THEN NULL ELSE SUM(dis2.sku_sales_quantity - dis2.sku_refund_quantity) END AS last_7_days_sku_sales --bc sales info starts from 09/09
FROM daily_inventory_sales_1 dis1
LEFT JOIN daily_inventory_sales_1 dis2
ON dis2.date BETWEEN dis1.last_7_days_begin AND DATEADD('DAY', -1, dis1.date)
AND dis1.location_id = dis2.location_id AND dis1.variant_id = dis2.variant_id
GROUP BY 1,2,3
                )
SELECT d.*
     , j.last_7_days_sku_sales
FROM daily_inventory_sales_1 d
LEFT JOIN join_table_2 j
ON d.date = j.date AND d.location_id = j.location_id AND d.variant_id = j.variant_id
;


-- 03: To calculate last 10 days on variant-location level
DROP TABLE IF EXISTS daily_inventory_sales_3;

CREATE TEMP TABLE daily_inventory_sales_3 AS

WITH join_table_3 AS (
SELECT dis1.date
     , dis1.location_id
     , dis1.variant_id
     , CASE
         WHEN dis1.date < '2020-09-12' THEN NULL ELSE SUM(dis2.sku_sales_quantity - dis2.sku_refund_quantity) END AS last_3_days_sku_sales --bc sales info starts from 09/09
FROM daily_inventory_sales_2 dis1
LEFT JOIN daily_inventory_sales_2 dis2
ON dis2.date BETWEEN dis1.last_3_days_begin AND DATEADD('DAY', -1, dis1.date)
AND dis1.location_id = dis2.location_id AND dis1.variant_id = dis2.variant_id
GROUP BY 1,2,3
                )
SELECT d.*
     , j.last_3_days_sku_sales
FROM daily_inventory_sales_2 d
LEFT JOIN join_table_3 j
ON d.date = j.date AND d.location_id = j.location_id AND d.variant_id = j.variant_id
;


-- 04: To calculate last 30 days on variant-location level
DELETE FROM grocery.stockupvw_daily_inventory_sales;

INSERT INTO grocery.stockupvw_daily_inventory_sales

WITH join_table_4 AS (
SELECT dis1.date
     , dis1.location_id
     , dis1.variant_id
     , CASE
         WHEN dis1.date < '2020-10-07' THEN NULL ELSE SUM(dis2.sku_sales_quantity - dis2.sku_refund_quantity) END AS last_28_days_sku_sales --bc sales info starts from 09/09
FROM daily_inventory_sales_3 dis1
LEFT JOIN daily_inventory_sales_3 dis2
ON dis2.date BETWEEN dis1.last_28_days_begin AND DATEADD('DAY', -1, dis1.date)
AND dis1.location_id = dis2.location_id AND dis1.variant_id = dis2.variant_id
GROUP BY 1,2,3
                )
SELECT d.*
     , j.last_28_days_sku_sales
FROM daily_inventory_sales_3 d
LEFT JOIN join_table_4 j
ON d.date = j.date AND d.location_id = j.location_id AND d.variant_id = j.variant_id
;

