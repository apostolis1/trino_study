--query40--naive
select  
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('1998-04-08' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('1998-04-08' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   cassandra.trino.catalog_sales left outer join cassandra.trino.catalog_returns on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,cassandra.trino.warehouse 
  ,redis.default.item
  ,redis.default.date_dim
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = concat('item:', cast(cs_item_sk as varchar))
 and cs_warehouse_sk    = w_warehouse_sk 
 and concat('date_dim:', cast(cs_sold_date_sk as varchar))    = d_date_sk
 and cast(d_date as date) between (cast ('1998-04-08' as date) - interval '30' day)
                and (cast ('1998-04-08' as date) + interval '30' day) 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
 fetch first 100 rows only;

-- end query 1 in stream 0 using template query40.tpl
