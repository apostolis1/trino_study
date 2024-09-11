--query15--naive
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
--end--query15--naive
