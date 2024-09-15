from QueryGen import QueryGen

u = QueryGen()
u.generate_count_and_dump()
print(u.generate_count_rows_stmts())