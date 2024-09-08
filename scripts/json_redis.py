from QueryGen import QueryGen


# parser = argparse.ArgumentParser()
# parser.add_argument("config", default="config.json")
# args = parser.parse_args()
# print(args.config)

u = QueryGen()
u.generate_redis_table_json_and_dump(include_keys=True)
