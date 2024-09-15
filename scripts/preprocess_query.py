from sqlglot import parse
from sqlglot.expressions import Identifier, Column, Table  
  
def transform(node, mapping):  
    if isinstance(node, (Identifier, Column, Table)) and node.args['this'] in mapping:  
        return type(node)(this=mapping[node.args['this']], **{arg: val for arg, val in node.args.items() if arg != 'this'})  
    return node 

sql = """
-- start query 1 in stream 0 using template query93.tpl
select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'Did not like the warranty') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
 fetch first 100 rows only;

-- end query 1 in stream 0 using template query93.tpl

"""

mapping = {'store_sales': 'cassandra.trino.store_sales', 'item': 'postgresql.public.item', 'customer_address': 'postgresql.public.customer_address', 'date_dim': 'postgresql.public.date_dim', 'catalog_sales': 'postgresql.public.catalog_sales', 'store_returns': 'redis.default.store_returns', 'catalog_returns': 'postgresql.public.catalog_returns', 'customer_demographics': 'postgresql.public.customer_demographics', 'web_returns': 'redis.default.web_returns', 'customer': 'postgresql.public.customer', 'time_dim': 'postgresql.public.time_dim', 'catalog_page': 'postgresql.public.catalog_page', 'household_demographics': 'postgresql.public.household_demographics', 'promotion': 'postgresql.public.promotion', 'store': 'postgresql.public.store', 'web_page': 'redis.default.web_page', 'web_site': 'postgresql.public.web_site', 'call_center': 'postgresql.public.call_center', 'reason': 'postgresql.public.reason', 'ship_mode': 'postgresql.public.ship_mode', 'warehouse': 'postgresql.public.warehouse', 'income_band': 'redis.default.income_band', 'inventory': 'cassandra.trino.inventory', 'web_sales': 'postgresql.public.web_sales'}

expressions = parse(sql)  
  
new_expressions = [expression.transform(lambda node: transform(node, mapping)) for expression in expressions]  
  
new_sql = "\n".join(expression.sql() for expression in new_expressions)   
  
print(new_sql)  