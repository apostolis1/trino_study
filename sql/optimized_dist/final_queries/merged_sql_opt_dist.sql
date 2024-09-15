--query5--opt--optsauto
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
        redis.default.store_returns
    ) AS salesreturns, 
    postgresql.public.date_dim, 
    postgresql.public.store 
  WHERE 
    date_sk = d_date_sk 
    AND d_date BETWEEN CAST('1998-08-04' AS DATE) 
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
        postgresql.public.catalog_sales 
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
        postgresql.public.catalog_returns
    ) AS salesreturns, 
    postgresql.public.date_dim, 
    postgresql.public.catalog_page 
  WHERE 
    date_sk = d_date_sk 
    AND d_date BETWEEN CAST('1998-08-04' AS DATE) 
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
        redis.default.web_returns 
        LEFT OUTER JOIN postgresql.public.web_sales ON (
          wr_item_sk = ws_item_sk 
          AND wr_order_number = ws_order_number
        )
    ) AS salesreturns, 
    postgresql.public.date_dim, 
    postgresql.public.web_site 
  WHERE 
    date_sk = d_date_sk 
    AND d_date BETWEEN CAST('1998-08-04' AS DATE) 
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
  id FETCH FIRST 100 ROWS ONLY 
--query5--opt--optsauto
;
--query15--opt--optsauto
SELECT 
  ca_zip, 
  SUM(cs_sales_price) 
FROM 
  postgresql.public.catalog_sales, 
  postgresql.public.customer, 
  postgresql.public.customer_address, 
  postgresql.public.date_dim 
WHERE 
  cs_bill_customer_sk = c_customer_sk 
  AND c_current_addr_sk = ca_address_sk 
  AND (
    SUBSTR(ca_zip, 1, 5) IN (
      '85669', '86197', '88274', '83405', 
      '86475', '85392', '85460', '80348', 
      '81792'
    ) 
    OR ca_state IN ('CA', 'WA', 'GA') 
    OR cs_sales_price > 500
  ) 
  AND cs_sold_date_sk = d_date_sk 
  AND d_qoy = 2 
  AND d_year = 2000 
GROUP BY 
  ca_zip 
ORDER BY 
  ca_zip FETCH FIRST 100 ROWS ONLY 
--end--query15--opt--optsauto
;
--query37--opt--optsauto
SELECT 
  i_item_id, 
  i_item_desc, 
  i_current_price 
FROM 
  postgresql.public.item, 
  cassandra.trino.inventory, 
  postgresql.public.date_dim, 
  postgresql.public.catalog_sales 
WHERE 
  i_current_price BETWEEN 22 
  AND 22 + 30 
  AND inv_item_sk = i_item_sk 
  AND d_date_sk = inv_date_sk 
  AND d_date BETWEEN CAST('2001-06-02' AS DATE) 
  AND (
    CAST('2001-06-02' AS DATE) + INTERVAL '60' day
  ) 
  AND i_manufact_id IN (678, 964, 918, 849) 
  AND inv_quantity_on_hand BETWEEN 100 
  AND 500 
  AND cs_item_sk = i_item_sk 
GROUP BY 
  i_item_id, 
  i_item_desc, 
  i_current_price 
ORDER BY 
  i_item_id FETCH FIRST 100 ROWS ONLY 
--end--query37--opt--optsauto

;
--query40--opt--optsauto
SELECT 
  w_state, 
  i_item_id, 
  SUM(
    CASE WHEN (
      CAST(d_date AS DATE) < CAST('1998-04-08' AS DATE)
    ) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END
  ) AS sales_before, 
  SUM(
    CASE WHEN (
      CAST(d_date AS DATE) >= CAST('1998-04-08' AS DATE)
    ) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END
  ) AS sales_after 
FROM 
  postgresql.public.catalog_sales 
  LEFT OUTER JOIN postgresql.public.catalog_returns ON (
    cs_order_number = cr_order_number 
    AND cs_item_sk = cr_item_sk
  ), 
  postgresql.public.warehouse, 
  postgresql.public.item, 
  postgresql.public.date_dim 
WHERE 
  i_current_price BETWEEN 0.99 
  AND 1.49 
  AND i_item_sk = cs_item_sk 
  AND cs_warehouse_sk = w_warehouse_sk 
  AND cs_sold_date_sk = d_date_sk 
  AND d_date BETWEEN (
    CAST('1998-04-08' AS DATE) - interval '30' day
  ) 
  AND (
    CAST('1998-04-08' AS DATE) + interval '30' day
  ) 
GROUP BY 
  w_state, 
  i_item_id 
ORDER BY 
  w_state, 
  i_item_id FETCH FIRST 100 ROWS ONLY
--end--query40--opt--optsauto
;
--query42--opt--optsauto
SELECT 
  dt.d_year, 
  postgresql.public.item.i_category_id, 
  postgresql.public.item.i_category, 
  SUM(ss_ext_sales_price) 
FROM 
  postgresql.public.date_dim AS dt, 
  cassandra.trino.store_sales, 
  postgresql.public.item 
WHERE 
  dt.d_date_sk = cassandra.trino.store_sales.ss_sold_date_sk 
  AND cassandra.trino.store_sales.ss_item_sk = postgresql.public.item.i_item_sk 
  AND postgresql.public.item.i_manager_id = 1 
  AND dt.d_moy = 12 
  AND dt.d_year = 1998 
GROUP BY 
  dt.d_year, 
  postgresql.public.item.i_category_id, 
  postgresql.public.item.i_category 
ORDER BY 
  SUM(ss_ext_sales_price) DESC, 
  dt.d_year, 
  postgresql.public.item.i_category_id, 
  postgresql.public.item.i_category FETCH FIRST 100 ROWS ONLY 
--end--query42--opt--optsauto
;
--query61--opt--optsauto
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
      postgresql.public.store, 
      postgresql.public.promotion, 
      postgresql.public.date_dim, 
      postgresql.public.customer, 
      postgresql.public.customer_address, 
      postgresql.public.item 
    WHERE 
      ss_sold_date_sk = d_date_sk 
      AND ss_store_sk = s_store_sk 
      AND ss_promo_sk = p_promo_sk 
      AND ss_customer_sk = c_customer_sk 
      AND ca_address_sk = c_current_addr_sk 
      AND ss_item_sk = i_item_sk 
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
      postgresql.public.store, 
      postgresql.public.date_dim, 
      postgresql.public.customer, 
      postgresql.public.customer_address, 
      postgresql.public.item 
    WHERE 
      ss_sold_date_sk = d_date_sk 
      AND ss_store_sk = s_store_sk 
      AND ss_customer_sk = c_customer_sk 
      AND ca_address_sk = c_current_addr_sk 
      AND ss_item_sk = i_item_sk 
      AND ca_gmt_offset = -7 
      AND i_category = 'Books' 
      AND s_gmt_offset = -7 
      AND d_year = 1999 
      AND d_moy = 11
  ) AS all_sales 
ORDER BY 
  promotions, 
  total FETCH FIRST 100 ROWS ONLY 
--end--query61--opt--optsauto
;
--query66--opt--optsauto
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
      postgresql.public.warehouse, 
      postgresql.public.date_dim, 
      postgresql.public.time_dim, 
      postgresql.public.ship_mode 
    WHERE 
      ws_warehouse_sk = w_warehouse_sk 
      AND ws_sold_date_sk = d_date_sk 
      AND ws_sold_time_sk = t_time_sk 
      AND ws_ship_mode_sk = sm_ship_mode_sk 
      AND d_year = 2002 
      AND t_time BETWEEN 49530 
      AND 49530 + 28800 
      AND sm_carrier IN ('DIAMOND', 'AIRBORNE') 
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
      postgresql.public.catalog_sales, 
      postgresql.public.warehouse, 
      postgresql.public.date_dim, 
      postgresql.public.time_dim, 
      postgresql.public.ship_mode 
    WHERE 
      cs_warehouse_sk = w_warehouse_sk 
      AND cs_sold_date_sk = d_date_sk 
      AND cs_sold_time_sk = t_time_sk 
      AND cs_ship_mode_sk = sm_ship_mode_sk 
      AND d_year = 2002 
      AND t_time BETWEEN 49530 
      AND 49530 + 28800 
      AND sm_carrier IN ('DIAMOND', 'AIRBORNE') 
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
--end--query66--opt--optsauto
;
--query84--optdist--optsauto
SELECT 
  c_customer_id AS customer_id, 
  COALESCE(c_last_name, '') || ', ' || COALESCE(c_first_name, '') AS customername 
FROM 
  postgresql.public.customer, 
  postgresql.public.customer_address, 
  postgresql.public.customer_demographics, 
  postgresql.public.household_demographics, 
  redis.default.income_band, 
  redis.default.store_returns 
WHERE 
  ca_city = 'Hopewell' 
  AND c_current_addr_sk = ca_address_sk 
  AND ib_lower_bound >= 32287 
  AND ib_upper_bound <= 32287 + 50000 
  AND cast(replace(ib_income_band_sk, 'income_band:', '') as int) = hd_income_band_sk 
  AND cd_demo_sk = c_current_cdemo_sk 
  AND hd_demo_sk = c_current_hdemo_sk 
  AND sr_cdemo_sk = cd_demo_sk 
ORDER BY 
  c_customer_id FETCH FIRST 100 ROWS ONLY 
--query84--optdist--optsauto
  ;
--query85--opt--optsauto
SELECT 
  SUBSTR(r_reason_desc, 1, 20), 
  AVG(ws_quantity), 
  AVG(wr_refunded_cash), 
  AVG(wr_fee) 
FROM 
  postgresql.public.web_sales, 
  redis.default.web_returns, 
  redis.default.web_page, 
  postgresql.public.customer_demographics AS cd1, 
  postgresql.public.customer_demographics AS cd2, 
  postgresql.public.customer_address, 
  postgresql.public.date_dim, 
  postgresql.public.reason 
WHERE 
  ws_web_page_sk = cast(replace(wp_web_page_sk, 'web_page:', '') as int)
  AND ws_item_sk = wr_item_sk 
  AND ws_order_number = wr_order_number 
  AND ws_sold_date_sk = d_date_sk 
  AND d_year = 1998 
  AND cd1.cd_demo_sk = wr_refunded_cdemo_sk 
  AND cd2.cd_demo_sk = wr_returning_cdemo_sk 
  AND ca_address_sk = wr_refunded_addr_sk 
  AND r_reason_sk = wr_reason_sk 
  AND (
    (
      cd1.cd_marital_status = 'M' 
      AND cd1.cd_marital_status = cd2.cd_marital_status 
      AND cd1.cd_education_status = '4 yr Degree' 
      AND cd1.cd_education_status = cd2.cd_education_status 
      AND ws_sales_price BETWEEN 100.00 
      AND 150.00
    ) 
    OR (
      cd1.cd_marital_status = 'D' 
      AND cd1.cd_marital_status = cd2.cd_marital_status 
      AND cd1.cd_education_status = 'Primary' 
      AND cd1.cd_education_status = cd2.cd_education_status 
      AND ws_sales_price BETWEEN 50.00 
      AND 100.00
    ) 
    OR (
      cd1.cd_marital_status = 'U' 
      AND cd1.cd_marital_status = cd2.cd_marital_status 
      AND cd1.cd_education_status = 'Advanced Degree' 
      AND cd1.cd_education_status = cd2.cd_education_status 
      AND ws_sales_price BETWEEN 150.00 
      AND 200.00
    )
  ) 
  AND (
    (
      ca_country = 'United States' 
      AND ca_state IN ('KY', 'GA', 'NM') 
      AND ws_net_profit BETWEEN 100 
      AND 200
    ) 
    OR (
      ca_country = 'United States' 
      AND ca_state IN ('MT', 'OR', 'IN') 
      AND ws_net_profit BETWEEN 150 
      AND 300
    ) 
    OR (
      ca_country = 'United States' 
      AND ca_state IN ('WI', 'MO', 'WV') 
      AND ws_net_profit BETWEEN 50 
      AND 250
    )
  ) 
GROUP BY 
  r_reason_desc 
ORDER BY 
  SUBSTR(r_reason_desc, 1, 20), 
  AVG(ws_quantity), 
  AVG(wr_refunded_cash), 
  AVG(wr_fee) FETCH FIRST 100 ROWS ONLY 
--end--query85--opt--optsauto
;
--query91--opt--optsauto
SELECT 
  cc_call_center_id AS Call_Center, 
  cc_name AS Call_Center_Name, 
  cc_manager AS Manager, 
  SUM(cr_net_loss) AS Returns_Loss 
FROM 
  postgresql.public.call_center, 
  postgresql.public.catalog_returns, 
  postgresql.public.date_dim, 
  postgresql.public.customer, 
  postgresql.public.customer_address, 
  postgresql.public.customer_demographics, 
  postgresql.public.household_demographics 
WHERE 
  cr_call_center_sk = cc_call_center_sk 
  AND cr_returned_date_sk = d_date_sk 
  AND cr_returning_customer_sk = c_customer_sk 
  AND cd_demo_sk = c_current_cdemo_sk 
  AND hd_demo_sk = c_current_hdemo_sk 
  AND ca_address_sk = c_current_addr_sk 
  AND d_year = 1999 
  AND d_moy = 11 
  AND (
    (
      cd_marital_status = 'M' 
      AND cd_education_status = 'Unknown'
    ) 
    OR (
      cd_marital_status = 'W' 
      AND cd_education_status = 'Advanced Degree'
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
--end--query91--opt--optsauto
;
--query93--opt--optsauto
SELECT 
  ss_customer_sk, 
  SUM(act_sales) AS sumsales 
FROM 
  (
    SELECT 
      ss_item_sk, 
      ss_ticket_number, 
      ss_customer_sk, 
      CASE WHEN NOT sr_return_quantity IS NULL THEN (ss_quantity - sr_return_quantity) * ss_sales_price ELSE (ss_quantity * ss_sales_price) END AS act_sales 
    FROM 
      cassandra.trino.store_sales 
      LEFT OUTER JOIN redis.default.store_returns ON (
        sr_item_sk = ss_item_sk 
        AND sr_ticket_number = ss_ticket_number
      ), 
      postgresql.public.reason 
    WHERE 
      sr_reason_sk = r_reason_sk 
      AND r_reason_desc = 'Did not like the warranty'
  ) AS t 
GROUP BY 
  ss_customer_sk 
ORDER BY 
  sumsales, 
  ss_customer_sk FETCH FIRST 100 ROWS ONLY 
--end--query93--opt--optsauto
;
