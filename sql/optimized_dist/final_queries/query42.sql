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
