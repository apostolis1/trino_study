--query37--naive
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from redis.default.item, postgres.public.inventory, redis.default.date_dim, cassandra.trino.catalog_sales
 where i_current_price between 22 and 22 + 30
 and concat("item:", inv_item_sk) = i_item_sk
 and d_date_sk=concat("date_dim:", inv_date_sk)
 and cast(d_date as date) between cast('2001-06-02' as date) and (cast('2001-06-02' as date) +  60 days)
 and i_manufact_id in (678,964,918,849)
 and inv_quantity_on_hand between 100 and 500
 and concat("item:", cs_item_sk) = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
  fetch first 100 rows only;

--end--query37--naive

