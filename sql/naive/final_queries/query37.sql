--query37--naive
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from redis.default.item, postgresql.public.inventory, redis.default.date_dim, cassandra.trino.catalog_sales
 where i_current_price between 22 and 22 + 30
 and inv_item_sk = cast(replace(i_item_sk, 'item:', '')as integer)
 and cast(replace(d_date_sk, 'date_dim:', '') as integer) = inv_date_sk
 and cast(d_date as date) between cast('2001-06-02' as date) and (cast('2001-06-02' as date) + interval '60' day)
 and i_manufact_id in (678,964,918,849)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = cast(replace(i_item_sk, 'item:', '')as integer)
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
  fetch first 100 rows only
  --end--query37--naive
;


