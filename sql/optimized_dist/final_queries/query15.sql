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
