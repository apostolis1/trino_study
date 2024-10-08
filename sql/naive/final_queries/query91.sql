--query91--naive
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
--end--query91--naive
;
