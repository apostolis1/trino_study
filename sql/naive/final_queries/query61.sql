--query61--naive
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
--end--query61--naive
;
