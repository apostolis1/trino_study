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
