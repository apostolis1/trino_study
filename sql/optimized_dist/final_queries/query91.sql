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
