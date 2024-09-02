from QueryGen import QueryGen
import argparse


# parser = argparse.ArgumentParser()
# parser.add_argument("config", default="config.json")
# args = parser.parse_args()
# print(args.config)

u = QueryGen()
u.generate_create_and_dump()
u.generate_insert_and_dump(detailed=True)
u.generate_drop_and_dump()
# u.generate_redis_table_json_and_dump()
u.generate_count_and_dump()