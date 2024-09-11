--query42--naive
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
--end--query42--naive
;
