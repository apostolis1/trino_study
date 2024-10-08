--query84--naive
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
--end--query84--naive
;
