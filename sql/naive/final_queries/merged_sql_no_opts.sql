SET SESSION query_max_execution_time = '90m';
SET SESSION join_reordering_strategy = 'NONE';
--query15--naive--noopts
select  ca_zip
       ,sum(cs_sales_price)
 from cassandra.trino.catalog_sales
     ,cassandra.trino.customer
     ,redis.default.customer_address
     ,redis.default.date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and concat('customer_address:' ,cast(c_current_addr_sk as varchar)) = ca_address_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = cast(replace(d_date_sk, 'date_dim:', '') as integer)
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip
 order by ca_zip
  fetch first 100 rows only;
--end--query15--naive--noopts
--query37--naive--noopts
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from redis.default.item, postgresql.public.inventory, redis.default.date_dim, cassandra.trino.catalog_sales
 where i_current_price between 22 and 22 + 30
 and inv_item_sk = cast(replace(i_item_sk, 'item:', '')as integer)
 and cast(replace(d_date_sk, 'date_dim:', '') as integer) = inv_date_sk
 and cast(d_date as date) between cast('2001-06-02' as date) and (cast('2001-06-02' as date) + interval '60' day)
 and i_manufact_id in (678,964,918,849)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = cast(replace(i_item_sk, 'item:', '')as integer)
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
  fetch first 100 rows only
  --end--query37--naive--noopts
;


--query40--naive--noopts
select  
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('1998-04-08' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('1998-04-08' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   cassandra.trino.catalog_sales left outer join cassandra.trino.catalog_returns on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,cassandra.trino.warehouse 
  ,redis.default.item
  ,redis.default.date_dim
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = concat('item:', cast(cs_item_sk as varchar))
 and cs_warehouse_sk    = w_warehouse_sk 
 and concat('date_dim:', cast(cs_sold_date_sk as varchar))    = d_date_sk
 and cast(d_date as date) between (cast ('1998-04-08' as date) - interval '30' day)
                and (cast ('1998-04-08' as date) + interval '30' day) 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
 fetch first 100 rows only
--end--query40--naive--noopts
 ;
--query42--naive--noopts
select  dt.d_year
 	,item.i_category_id
 	,item.i_category
 	,sum(ss_ext_sales_price)
 from 	redis.default.date_dim dt
 	,cassandra.trino.store_sales
 	,redis.default.item
 where cast(replace(dt.d_date_sk, 'date_dim:', '') as integer) = store_sales.ss_sold_date_sk
 	and store_sales.ss_item_sk = cast(replace(item.i_item_sk, 'item:', '')as integer)
 	and item.i_manager_id = 1  	
 	and dt.d_moy=12
 	and dt.d_year=1998
 group by 	dt.d_year
 		,item.i_category_id
 		,item.i_category
 order by       sum(ss_ext_sales_price) desc,dt.d_year
 		,item.i_category_id
 		,item.i_category
 fetch first 100 rows only 
--end--query42--naive--noopts
;
--query61--naive--noopts
SELECT 
  promotions, 
  total, 
  CAST(
    promotions AS DECIMAL(15, 4)
  ) / CAST(
    total AS DECIMAL(15, 4)
  ) * 100 
FROM 
  (
    SELECT 
      SUM(ss_ext_sales_price) AS promotions 
    FROM 
      cassandra.trino.store_sales, 
      cassandra.trino.store, 
      cassandra.trino.promotion, 
      redis.default.date_dim, 
      cassandra.trino.customer, 
      redis.default.customer_address, 
      redis.default.item 
    WHERE 
      ss_sold_date_sk = cast(replace(d_date_sk, 'date_dim:', '') as integer) 
      AND ss_store_sk = s_store_sk 
      AND ss_promo_sk = p_promo_sk 
      AND ss_customer_sk = c_customer_sk 
      AND cast(replace(ca_address_sk, 'customer_address:', '') as integer) = c_current_addr_sk 
      AND ss_item_sk = cast(replace(item.i_item_sk, 'item:', '')as integer) 
      AND ca_gmt_offset = -7 
      AND i_category = 'Books' 
      AND (
        p_channel_dmail = 'Y' 
        OR p_channel_email = 'Y' 
        OR p_channel_tv = 'Y'
      ) 
      AND s_gmt_offset = -7 
      AND d_year = 1999 
      AND d_moy = 11
  ) AS promotional_sales, 
  (
    SELECT 
      SUM(ss_ext_sales_price) AS total 
    FROM 
      cassandra.trino.store_sales, 
      cassandra.trino.store, 
      redis.default.date_dim, 
      cassandra.trino.customer, 
      redis.default.customer_address, 
      redis.default.item 
    WHERE 
      ss_sold_date_sk = cast(replace(d_date_sk, 'date_dim:', '') as integer)
      AND ss_store_sk = s_store_sk 
      AND ss_customer_sk = c_customer_sk 
      AND cast(replace(ca_address_sk, 'customer_address:', '') as integer) = c_current_addr_sk 
      AND ss_item_sk = cast(replace(item.i_item_sk, 'item:', '')as integer) 
      AND ca_gmt_offset = -7 
      AND i_category = 'Books' 
      AND s_gmt_offset = -7 
      AND d_year = 1999 
      AND d_moy = 11
  ) AS all_sales 
ORDER BY 
  promotions, 
  total FETCH FIRST 100 ROWS ONLY
--end--query61--naive--noopts
;
--query66--naive--noopts
SELECT 
  w_warehouse_name, 
  w_warehouse_sq_ft, 
  w_city, 
  w_county, 
  w_state, 
  w_country, 
  ship_carriers, 
  year, 
  SUM(jan_sales) AS jan_sales, 
  SUM(feb_sales) AS feb_sales, 
  SUM(mar_sales) AS mar_sales, 
  SUM(apr_sales) AS apr_sales, 
  SUM(may_sales) AS may_sales, 
  SUM(jun_sales) AS jun_sales, 
  SUM(jul_sales) AS jul_sales, 
  SUM(aug_sales) AS aug_sales, 
  SUM(sep_sales) AS sep_sales, 
  SUM(oct_sales) AS oct_sales, 
  SUM(nov_sales) AS nov_sales, 
  SUM(dec_sales) AS dec_sales, 
  SUM(jan_sales / w_warehouse_sq_ft) AS jan_sales_per_sq_foot, 
  SUM(feb_sales / w_warehouse_sq_ft) AS feb_sales_per_sq_foot, 
  SUM(mar_sales / w_warehouse_sq_ft) AS mar_sales_per_sq_foot, 
  SUM(apr_sales / w_warehouse_sq_ft) AS apr_sales_per_sq_foot, 
  SUM(may_sales / w_warehouse_sq_ft) AS may_sales_per_sq_foot, 
  SUM(jun_sales / w_warehouse_sq_ft) AS jun_sales_per_sq_foot, 
  SUM(jul_sales / w_warehouse_sq_ft) AS jul_sales_per_sq_foot, 
  SUM(aug_sales / w_warehouse_sq_ft) AS aug_sales_per_sq_foot, 
  SUM(sep_sales / w_warehouse_sq_ft) AS sep_sales_per_sq_foot, 
  SUM(oct_sales / w_warehouse_sq_ft) AS oct_sales_per_sq_foot, 
  SUM(nov_sales / w_warehouse_sq_ft) AS nov_sales_per_sq_foot, 
  SUM(dec_sales / w_warehouse_sq_ft) AS dec_sales_per_sq_foot, 
  SUM(jan_net) AS jan_net, 
  SUM(feb_net) AS feb_net, 
  SUM(mar_net) AS mar_net, 
  SUM(apr_net) AS apr_net, 
  SUM(may_net) AS may_net, 
  SUM(jun_net) AS jun_net, 
  SUM(jul_net) AS jul_net, 
  SUM(aug_net) AS aug_net, 
  SUM(sep_net) AS sep_net, 
  SUM(oct_net) AS oct_net, 
  SUM(nov_net) AS nov_net, 
  SUM(dec_net) AS dec_net 
FROM 
  (
    SELECT 
      w_warehouse_name, 
      w_warehouse_sq_ft, 
      w_city, 
      w_county, 
      w_state, 
      w_country, 
      'DIAMOND' || ',' || 'AIRBORNE' AS ship_carriers, 
      d_year AS year, 
      SUM(
        CASE WHEN d_moy = 1 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS jan_sales, 
      SUM(
        CASE WHEN d_moy = 2 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS feb_sales, 
      SUM(
        CASE WHEN d_moy = 3 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS mar_sales, 
      SUM(
        CASE WHEN d_moy = 4 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS apr_sales, 
      SUM(
        CASE WHEN d_moy = 5 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS may_sales, 
      SUM(
        CASE WHEN d_moy = 6 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS jun_sales, 
      SUM(
        CASE WHEN d_moy = 7 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS jul_sales, 
      SUM(
        CASE WHEN d_moy = 8 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS aug_sales, 
      SUM(
        CASE WHEN d_moy = 9 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS sep_sales, 
      SUM(
        CASE WHEN d_moy = 10 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS oct_sales, 
      SUM(
        CASE WHEN d_moy = 11 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS nov_sales, 
      SUM(
        CASE WHEN d_moy = 12 THEN ws_sales_price * ws_quantity ELSE 0 END
      ) AS dec_sales, 
      SUM(
        CASE WHEN d_moy = 1 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS jan_net, 
      SUM(
        CASE WHEN d_moy = 2 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS feb_net, 
      SUM(
        CASE WHEN d_moy = 3 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS mar_net, 
      SUM(
        CASE WHEN d_moy = 4 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS apr_net, 
      SUM(
        CASE WHEN d_moy = 5 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS may_net, 
      SUM(
        CASE WHEN d_moy = 6 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS jun_net, 
      SUM(
        CASE WHEN d_moy = 7 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS jul_net, 
      SUM(
        CASE WHEN d_moy = 8 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS aug_net, 
      SUM(
        CASE WHEN d_moy = 9 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS sep_net, 
      SUM(
        CASE WHEN d_moy = 10 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS oct_net, 
      SUM(
        CASE WHEN d_moy = 11 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS nov_net, 
      SUM(
        CASE WHEN d_moy = 12 THEN ws_net_paid_inc_tax * ws_quantity ELSE 0 END
      ) AS dec_net 
    FROM 
      postgresql.public.web_sales, 
      cassandra.trino.warehouse, 
      redis.default.date_dim, 
      cassandra.trino.time_dim, 
      cassandra.trino.ship_mode 
    WHERE 
      ws_warehouse_sk = w_warehouse_sk 
      AND ws_sold_date_sk = cast(replace(d_date_sk, 'date_dim:', '') as integer) 
      AND ws_sold_time_sk = t_time_sk 
      AND ws_ship_mode_sk = sm_ship_mode_sk 
      AND d_year = 2002 
      AND t_time BETWEEN 49530 
      AND 49530 + 28800 
      AND trim(sm_carrier) IN ('DIAMOND', 'AIRBORNE') 
    GROUP BY 
      w_warehouse_name, 
      w_warehouse_sq_ft, 
      w_city, 
      w_county, 
      w_state, 
      w_country, 
      d_year 
    UNION ALL 
    SELECT 
      w_warehouse_name, 
      w_warehouse_sq_ft, 
      w_city, 
      w_county, 
      w_state, 
      w_country, 
      'DIAMOND' || ',' || 'AIRBORNE' AS ship_carriers, 
      d_year AS year, 
      SUM(
        CASE WHEN d_moy = 1 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS jan_sales, 
      SUM(
        CASE WHEN d_moy = 2 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS feb_sales, 
      SUM(
        CASE WHEN d_moy = 3 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS mar_sales, 
      SUM(
        CASE WHEN d_moy = 4 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS apr_sales, 
      SUM(
        CASE WHEN d_moy = 5 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS may_sales, 
      SUM(
        CASE WHEN d_moy = 6 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS jun_sales, 
      SUM(
        CASE WHEN d_moy = 7 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS jul_sales, 
      SUM(
        CASE WHEN d_moy = 8 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS aug_sales, 
      SUM(
        CASE WHEN d_moy = 9 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS sep_sales, 
      SUM(
        CASE WHEN d_moy = 10 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS oct_sales, 
      SUM(
        CASE WHEN d_moy = 11 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS nov_sales, 
      SUM(
        CASE WHEN d_moy = 12 THEN cs_ext_sales_price * cs_quantity ELSE 0 END
      ) AS dec_sales, 
      SUM(
        CASE WHEN d_moy = 1 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS jan_net, 
      SUM(
        CASE WHEN d_moy = 2 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS feb_net, 
      SUM(
        CASE WHEN d_moy = 3 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS mar_net, 
      SUM(
        CASE WHEN d_moy = 4 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS apr_net, 
      SUM(
        CASE WHEN d_moy = 5 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS may_net, 
      SUM(
        CASE WHEN d_moy = 6 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS jun_net, 
      SUM(
        CASE WHEN d_moy = 7 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS jul_net, 
      SUM(
        CASE WHEN d_moy = 8 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS aug_net, 
      SUM(
        CASE WHEN d_moy = 9 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS sep_net, 
      SUM(
        CASE WHEN d_moy = 10 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS oct_net, 
      SUM(
        CASE WHEN d_moy = 11 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS nov_net, 
      SUM(
        CASE WHEN d_moy = 12 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END
      ) AS dec_net 
    FROM 
      cassandra.trino.catalog_sales, 
      cassandra.trino.warehouse, 
      redis.default.date_dim, 
      cassandra.trino.time_dim, 
      cassandra.trino.ship_mode 
    WHERE 
      cs_warehouse_sk = w_warehouse_sk 
      AND cs_sold_date_sk = cast(replace(d_date_sk, 'date_dim:', '') as integer) 
      AND cs_sold_time_sk = t_time_sk 
      AND cs_ship_mode_sk = sm_ship_mode_sk 
      AND d_year = 2002 
      AND t_time BETWEEN 49530 
      AND 49530 + 28800 
      AND trim(sm_carrier) IN ('DIAMOND', 'AIRBORNE') 
    GROUP BY 
      w_warehouse_name, 
      w_warehouse_sq_ft, 
      w_city, 
      w_county, 
      w_state, 
      w_country, 
      d_year
  ) AS x 
GROUP BY 
  w_warehouse_name, 
  w_warehouse_sq_ft, 
  w_city, 
  w_county, 
  w_state, 
  w_country, 
  ship_carriers, 
  year 
ORDER BY 
  w_warehouse_name FETCH FIRST 100 ROWS ONLY
--end--query66--naive--noopts
;

--query84--naive--noopts
SELECT 
  c_customer_id AS customer_id, 
  COALESCE(trim(c_last_name), '') || ', ' || COALESCE(trim(c_first_name), '') AS customername 
FROM 
  cassandra.trino.customer, 
  redis.default.customer_address, 
  cassandra.trino.customer_demographics, 
  cassandra.trino.household_demographics, 
  cassandra.trino.income_band, 
  cassandra.trino.store_returns 
WHERE 
  ca_city = 'Hopewell' 
  AND c_current_addr_sk = cast(replace(ca_address_sk, 'customer_address:', '') as integer) 
  AND ib_lower_bound >= 32287 
  AND ib_upper_bound <= 32287 + 50000 
  AND ib_income_band_sk = hd_income_band_sk 
  AND cd_demo_sk = c_current_cdemo_sk 
  AND hd_demo_sk = c_current_hdemo_sk 
  AND sr_cdemo_sk = cd_demo_sk 
ORDER BY 
  c_customer_id FETCH FIRST 100 ROWS ONLY 
--end--query84--naive--noopts
;
--query85--naive--noopts
select  substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
 from postgresql.public.web_sales, cassandra.trino.web_returns, cassandra.trino.web_page, cassandra.trino.customer_demographics cd1,
      cassandra.trino.customer_demographics cd2, redis.default.customer_address, redis.default.date_dim, cassandra.trino.reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = cast(replace(d_date_sk, 'date_dim:', '') as integer) and d_year = 1998
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and cast(replace(ca_address_sk, 'customer_address:', '') as integer) = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     trim(cd1.cd_education_status) = '4 yr Degree'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'D'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     trim(cd1.cd_education_status) = 'Primary' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'U'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     trim(cd1.cd_education_status) = 'Advanced Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('KY', 'GA', 'NM')
     and ws_net_profit between 100 and 200  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('MT', 'OR', 'IN')
     and ws_net_profit between 150 and 300  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('WI', 'MO', 'WV')
     and ws_net_profit between 50 and 250  
    )
   )
group by r_reason_desc
order by substr(r_reason_desc,1,20)
        ,avg(ws_quantity)
        ,avg(wr_refunded_cash)
        ,avg(wr_fee)
 fetch first 100 rows only
--end--query85--naive--noopts
;
--query91--naive--noopts
SELECT 
  cc_call_center_id AS Call_Center, 
  cc_name AS Call_Center_Name, 
  cc_manager AS Manager, 
  SUM(cr_net_loss) AS Returns_Loss 
FROM 
  cassandra.trino.call_center, 
  cassandra.trino.catalog_returns, 
  redis.default.date_dim, 
  cassandra.trino.customer, 
  redis.default.customer_address, 
  cassandra.trino.customer_demographics, 
  cassandra.trino.household_demographics 
WHERE 
  cr_call_center_sk = cc_call_center_sk 
  AND cr_returned_date_sk = cast(replace(d_date_sk, 'date_dim:', '') as integer) 
  AND cr_returning_customer_sk = c_customer_sk 
  AND cd_demo_sk = c_current_cdemo_sk 
  AND hd_demo_sk = c_current_hdemo_sk 
  AND cast(replace(ca_address_sk, 'customer_address:', '') as integer) = c_current_addr_sk 
  AND d_year = 1999 
  AND d_moy = 11 
  AND (
    (
      cd_marital_status = 'M' 
      AND trim(cd_education_status) = 'Unknown'
    ) 
    OR (
      cd_marital_status = 'W' 
      AND trim(cd_education_status) = 'Advanced Degree'
    )
  ) 
  AND hd_buy_potential LIKE '0-500%' 
  AND ca_gmt_offset = -7 
GROUP BY 
  cc_call_center_id, 
  cc_name, 
  cc_manager, 
  cd_marital_status, 
  cd_education_status 
ORDER BY 
  SUM(cr_net_loss) DESC
--end--query91--naive--noopts
;
--query93--naive--noopts
select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from cassandra.trino.store_sales left outer join cassandra.trino.store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,cassandra.trino.reason
            where sr_reason_sk = r_reason_sk
              and trim(r_reason_desc) = 'Did not like the warranty') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
 fetch first 100 rows only
--end--query93--naive--noopts
;
--query5--naive--noopts
WITH ssr AS (
  SELECT 
    s_store_id, 
    SUM(sales_price) AS sales, 
    SUM(profit) AS profit, 
    SUM(return_amt) AS returns, 
    SUM(net_loss) AS profit_loss 
  FROM 
    (
      SELECT 
        ss_store_sk AS store_sk, 
        ss_sold_date_sk AS date_sk, 
        ss_ext_sales_price AS sales_price, 
        ss_net_profit AS profit, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS return_amt, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS net_loss 
      FROM 
        cassandra.trino.store_sales 
      UNION ALL 
      SELECT 
        sr_store_sk AS store_sk, 
        sr_returned_date_sk AS date_sk, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS sales_price, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS profit, 
        sr_return_amt AS return_amt, 
        sr_net_loss AS net_loss 
      FROM 
        cassandra.trino.store_returns
    ) AS salesreturns, 
    redis.default.date_dim, 
    cassandra.trino.store 
  WHERE 
    date_sk =  cast(replace(d_date_sk, 'date_dim:', '') as integer) 
    AND cast(d_date as date) BETWEEN CAST('1998-08-04' AS DATE) 
    AND (
      CAST('1998-08-04' AS DATE) + interval '14' day
    ) 
    AND store_sk = s_store_sk 
  GROUP BY 
    s_store_id
), 
csr AS (
  SELECT 
    cp_catalog_page_id, 
    SUM(sales_price) AS sales, 
    SUM(profit) AS profit, 
    SUM(return_amt) AS returns, 
    SUM(net_loss) AS profit_loss 
  FROM 
    (
      SELECT 
        cs_catalog_page_sk AS page_sk, 
        cs_sold_date_sk AS date_sk, 
        cs_ext_sales_price AS sales_price, 
        cs_net_profit AS profit, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS return_amt, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS net_loss 
      FROM 
        cassandra.trino.catalog_sales 
      UNION ALL 
      SELECT 
        cr_catalog_page_sk AS page_sk, 
        cr_returned_date_sk AS date_sk, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS sales_price, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS profit, 
        cr_return_amount AS return_amt, 
        cr_net_loss AS net_loss 
      FROM 
        cassandra.trino.catalog_returns
    ) AS salesreturns, 
    redis.default.date_dim, 
    cassandra.trino.catalog_page 
  WHERE 
    date_sk =  cast(replace(d_date_sk, 'date_dim:', '') as integer) 
    AND cast(d_date as date) BETWEEN CAST('1998-08-04' AS DATE) 
    AND (
      CAST('1998-08-04' AS DATE) + interval '14' day
    ) 
    AND page_sk = cp_catalog_page_sk 
  GROUP BY 
    cp_catalog_page_id
), 
wsr AS (
  SELECT 
    web_site_id, 
    SUM(sales_price) AS sales, 
    SUM(profit) AS profit, 
    SUM(return_amt) AS returns, 
    SUM(net_loss) AS profit_loss 
  FROM 
    (
      SELECT 
        ws_web_site_sk AS wsr_web_site_sk, 
        ws_sold_date_sk AS date_sk, 
        ws_ext_sales_price AS sales_price, 
        ws_net_profit AS profit, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS return_amt, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS net_loss 
      FROM 
        postgresql.public.web_sales 
      UNION ALL 
      SELECT 
        ws_web_site_sk AS wsr_web_site_sk, 
        wr_returned_date_sk AS date_sk, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS sales_price, 
        CAST(
          0 AS DECIMAL(7, 2)
        ) AS profit, 
        wr_return_amt AS return_amt, 
        wr_net_loss AS net_loss 
      FROM 
        cassandra.trino.web_returns 
        LEFT OUTER JOIN postgresql.public.web_sales ON (
          wr_item_sk = ws_item_sk 
          AND wr_order_number = ws_order_number
        )
    ) AS salesreturns, 
    redis.default.date_dim, 
    cassandra.trino.web_site 
  WHERE 
    date_sk =  cast(replace(d_date_sk, 'date_dim:', '') as integer) 
    AND cast(d_date as date) BETWEEN CAST('1998-08-04' AS DATE) 
    AND (
      CAST('1998-08-04' AS DATE) + interval '14' day
    ) 
    AND wsr_web_site_sk = web_site_sk 
  GROUP BY 
    web_site_id
) 
SELECT 
  channel, 
  id, 
  SUM(sales) AS sales, 
  SUM(returns) AS returns, 
  SUM(profit) AS profit 
FROM 
  (
    SELECT 
      'store channel' AS channel, 
      'store' || s_store_id AS id, 
      sales, 
      returns, 
      (profit - profit_loss) AS profit 
    FROM 
      ssr 
    UNION ALL 
    SELECT 
      'catalog channel' AS channel, 
      'catalog_page' || cp_catalog_page_id AS id, 
      sales, 
      returns, 
      (profit - profit_loss) AS profit 
    FROM 
      csr 
    UNION ALL 
    SELECT 
      'web channel' AS channel, 
      'web_site' || web_site_id AS id, 
      sales, 
      returns, 
      (profit - profit_loss) AS profit 
    FROM 
      wsr
  ) AS x 
GROUP BY 
  ROLLUP (channel, id) 
ORDER BY 
  channel, 
  id FETCH FIRST 100 ROWS ONLY ;

--end--query5--naive--noopts

