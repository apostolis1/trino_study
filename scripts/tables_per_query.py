import sqlglot
import sqlglot.expressions as exp
import os
import pandas as pd


QUERY_FOLDER = "/home/apostolis/Projects/bigdata/selected"
# QUERY_FOLDER = "/home/apostolis/Projects/bigdata/queries_templates"


tables = ['customer', 'store_sales', 'web_site', 'web_page', 'household_demographics', 'promotion', 'date_dim', 'store_returns', 'customer_address', 'ship_mode', 'customer_demographics', 'item', 'warehouse', 'catalog_sales', 'income_band', 'web_sales', 'call_center', 'time_dim', 'inventory', 'catalog_returns', 'reason', 'store', 'catalog_page', 'web_returns']

uses_tables = {}
for file in os.listdir(QUERY_FOLDER):
    with open(os.path.join(QUERY_FOLDER, file), "r") as f:
        tables_used = []
        for table in sqlglot.parse_one(f.read()).find_all(exp.Table):
            if table.name in tables:
                # print(file, table.name)
                tables_used.append(table.name)
        uses_tables[file] = tables_used

# print(uses_tables)
res = []
res.append(["Query"])
res[0].extend(tables)

for query, tablesUsed in uses_tables.items():
    bool_tables = [query]

    for table in tables:
        if table in tablesUsed:
            bool_tables.append("X")
        else:
            bool_tables.append(" ")
    res.append(bool_tables)

# for query, tablesUsed in uses_tables.items():
#     if "income_band" in tablesUsed: # and "income_band" in tablesUsed:
#         print(f"{query} is good")

df = pd.DataFrame(res)
print(df)
df.to_csv("./final_queries.csv")  
df_t = df.transpose()
df_t.to_csv("./final_queries_transposed.csv")  


