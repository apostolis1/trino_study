import json
import re


def get_redis_type(type: str):
    type_lower = type.lower()
    if type_lower == 'integer' or "decimal" in type_lower:
        return "DOUBLE"
    if type_lower == 'date':
        return "VARCHAR(10)"
    if "char" in type_lower and "varchar" not in type_lower:
        # TODO this can be better written with a regex of lookbehind
        return type_lower.replace("char", "VARCHAR")
    return type.upper()

def get_cassandra_type(type: str):
    type_lower = type.lower()
    if "decimal" in type_lower:
        return "DOUBLE"
    if "varchar" in type_lower:
        return "VARCHAR"
    if type_lower == "integer":
        return "INT"
    if "char" in type_lower and "varchar" not in type_lower:
        # Cassandra doesn't allow a param in varchar, so we don't want varchar(10), we want varchar
        return "VARCHAR"
    return type.upper()

def get_postgres_type(type: str):
    return type.upper()


class Config:
    def __init__(self, config):
        self.config = config
    
    def get_tpcds_template(self):
        return self.config["TPCDS_TEMPLATE"]

    def get_table_dist(self):
        return self.config["TABLE_DIST"]
    
    def get_create_out(self):
        return self.config["CREATE_OUT"]
    
    def get_insert_out(self):
        return self.config["INSERT_OUT"]

    def get_drop_out(self):
        return self.config["DROP_OUT"]
    
    def get_count_out(self):
        return self.config["COUNT_OUT"]    
    
    def get_redis_json_dir(self):
        return self.config["REDIS_JSON_DIR"]

    def get_benchmark(self):
        return self.config["BENCHMARK"]

class SQLTable:
    def __init__(self, name, cols, pkey) -> None:
        self.name = name
        # a dict of col_name, col_data_type
        # Make sure the cols are in lowercase
        self.cols = {k: v.lower() for k,v in cols.items()}
        self.pkey = pkey # a list of the col names that make the pkey
    
    def pprint(self):
        print(f"SQLTable: {self.name}:")
        print(f"\tprimary key\t{self.pkey}")
        for col_name, col_type in self.cols.items():
            print(f"\t{col_name}\t{col_type}")
    
    def get_key_str(self, db: str) -> str:
        """
        Returns a str to be used in the create table statement to specify the primary key
        db can be one of: 'postgres', 'cassandra'
        """
        if self.pkey is None:
            return None
        if db == 'postgres':
            return ",".join(self.pkey).replace("'", "")
        if db == 'cassandra':
            # TODO: Check if in cassandra the pkey is meaningful if we have a single instance and not a cluster
            # It looks that it is from a quick serach, see Cassandra.md
            return f"({','.join(self.pkey)})"

        raise Exception(f"Unexpected db: {db}")

    def get_key_json(self) -> dict:
        if self.pkey is not None:
            if len(self.pkey) > 1:
                d = {
                    "dataFormat": "raw",
                    "fields": [
                        {
                            "name": "pkey",
                            "type": "VARCHAR"
                        }
                    ]
                }
            else:
                fields = []
                for col in self.pkey:
                    fields.append({
                        "name": col,
                        "type": "VARCHAR"
                    })
                d = {
                    "dataFormat": "raw",
                    "fields": fields
                }
            return d

    def get_fields(self) -> list:
        fields = []
        if not self.pkey:
            print(self.name)
        for col_name, col_type in self.cols.items():
            # The field should not be included when
            # 1) It is the only pkey

            if not (len(self.pkey) == 1 and col_name == self.pkey[0]): 
                fields.append({
                    "name": col_name,
                    "mapping": col_name,
                    "type": get_redis_type(col_type)
                })
        return fields

    def has_date_col(self):
        for col_type in self.cols.values():
            if col_type == "date":
                return True
        return False



class QueryGen:

    SCHEMAS = {
        # "redis": "redisearch.default",
        "redis": "redis.default",
        "postgres": "postgresql.public",
        "cassandra": "cassandra.trino"
    }

    TABLES_TO_EXCLUDE = set(['dbgen_version'])

    def __init__(self, config_path="config.json"):
        self.config: Config = self.get_config_from_file(config_path)
        self.sql_tables = self.create_sql_tables()
        self.mapping = self.get_table_dist()

    def get_config_from_file(self, config_path) -> Config:
        with open(config_path, "r") as f:
            return Config(json.load(f))

    def get_table_dist(self):
        with open(self.config.get_table_dist()) as f:
            table_dist = json.load(f)
        return table_dist

    def get_sql_table_to_stmt(self):
        stmts = self.get_sql_stmts()

        res = {}
        for stmt in stmts:
            for line in stmt.splitlines():
                if "create table" in line:
                    table_name = line.split()[2]
                    break
            res[table_name] = stmt
        return res

    def get_sql_stmts(self):
        with open(self.config.get_tpcds_template()) as f:
            data = f.read()
            sql_commands = data.split(';')[:-1]
        return sql_commands

    def get_full_name(self, table_name):
        db = self.mapping[table_name]
        return self.SCHEMAS[db] + "." + table_name
    
    
    def dump_to_file(self, sql_stmts, output_path, add_new_line=False):
        with open(output_path, "w+") as f:
            if add_new_line:
                delim = ";\n"
            else:
                delim = ";"
            f.writelines(line + delim for line in sql_stmts)

    def generate_create(self) -> list:
        """
        Returns a list of strings. Each string is a create table statement
        """
        create_statements = []
        for table in self.sql_tables:
            if table.name not in self.TABLES_TO_EXCLUDE:
                create_statements.append(self.create_from_table(table, True))
        return create_statements


    def create_from_table(self, sql_table: SQLTable, include_pkey=False) -> str:
        """
        Returns the corresponding create sql statement for a given table
        Uses the self.mapping to find where the table is placeed and map the types accordingly
        """
        db = self.mapping[sql_table.name]
        table_full_name = self.get_full_name(sql_table.name)
        cols = [f"{col_name} {self.get_fixed_col_type(col_type, db)}" for col_name, col_type in sql_table.cols.items()]
        if include_pkey:
            pkey_str = sql_table.get_key_str(db)
            if pkey_str is not None:
                cols.append(f"PRIMARY KEY({pkey_str})")
        cols_str = ",\n".join(cols)
        stmt = f"CREATE TABLE IF NOT EXISTS {table_full_name} \n(\n{cols_str}\n)"
        return stmt 
    
    def get_fixed_col_type(self, col_type: str, db: str) -> str:
        """
        Returns the final type that should be used based on the db
        db can be one of 'redis', 'postgres', 'cassandra' 
        """
        if db == 'redis':
            return get_redis_type(col_type)
        if db == 'postgres':
            return get_postgres_type(col_type)
        if db == 'cassandra':
            return get_cassandra_type(col_type)
        raise Exception(f"Unexpected db: {db}")

    def generate_redis_insert_stmt(self, sql_table: SQLTable, full_table_name: str, benchmark_table_name: str) -> str:
        # If there is no col with data type 'date', then there is no reason for casting and we can do a simple select * stmt
        if not sql_table.has_date_col():
            return f"INSERT INTO {full_table_name} SELECT * FROM {benchmark_table_name}"
        col_names = []
        cast_names = []
        for col_name, col_type in sql_table.cols.items():
            redis_type = get_redis_type(col_type)
            col_names.append(col_name)
            if col_type == "date":
                cast_names.append(f"cast({col_name} as varchar)")
            else:
                cast_names.append(col_name)
        columns = ",".join(col_names)
        columns_with_cast = ",".join(cast_names)
        stmt = f"INSERT INTO {full_table_name} ({columns}) SELECT {columns_with_cast} FROM {benchmark_table_name}"
        return stmt


    def generate_insert_stmts(self, detailed) -> list:
        stmts = []
        for sql_table in self.sql_tables:
            if sql_table.name in self.TABLES_TO_EXCLUDE:
                continue
            db = self.mapping[sql_table.name]
            full_table_name = self.get_full_name(sql_table.name)
            benchmark_table_name = f"tpcds.{self.config.get_benchmark()}.{sql_table.name}"
            # Redis doesn't support 'date' data type
            # For this reason we have to cast the 'date' cols if they exist to 'varchar'
            if db == "redis":
                stmt = self.generate_redis_insert_stmt(sql_table, full_table_name, benchmark_table_name)
            elif not detailed:
                stmt = f"INSERT INTO {full_table_name} SELECT * FROM {benchmark_table_name}"
            else:
                stmt = f"INSERT INTO {full_table_name} ({','.join(sql_table.cols.keys())}) SELECT {','.join(sql_table.cols.keys())} FROM {benchmark_table_name}"
            stmts.append(stmt)
        return stmts

    def generate_drop_stmts(self) -> list:
        stmts = []
        for table_name, db in self.mapping.items():
            full_table_name = self.get_full_name(table_name)
            stmt = f"DROP TABLE IF EXISTS {full_table_name}"
            stmts.append(stmt)
        return stmts

    def generate_count_stmts(self) -> list:
        compares = []
        for table_name, db in self.mapping.items():
            full_table_name = self.get_full_name(table_name)
            cmp = f"(SELECT count(*) from tpcds.{self.config.get_benchmark()}.{table_name})=(SELECT count(*) from {full_table_name}) AS {table_name}"
            compares.append(cmp)
        comp_str = ",".join(compares)
        count_stmt = f"SELECT {comp_str};"
        return [count_stmt]

    def generate_create_and_dump(self):
        st = self.generate_create()
        self.dump_to_file(st, self.config.get_create_out(), True)

    def generate_insert_and_dump(self, detailed=False) -> None:
        st = self.generate_insert_stmts(detailed=detailed)
        self.dump_to_file(st, self.config.get_insert_out(), True)
        
    def generate_drop_and_dump(self)-> None:
        st = self.generate_drop_stmts()
        self.dump_to_file(st, self.config.get_drop_out(), True)
    
    def generate_count_and_dump(self) -> None:
        st = self.generate_count_stmts()
        self.dump_to_file(st, self.config.get_count_out())
    

    def json_from_sqltable(self, sql_table: SQLTable, include_keys):
        d = {
            "tableName": sql_table.name,
            "value": {
                "dataFormat": "hash",
                "fields": sql_table.get_fields()
            }
        }
        if include_keys and sql_table.pkey is not None:
            d["key"] = sql_table.get_key_json()
        return d


    def generate_redis_table_json_and_dump(self, include_keys=False):
        """
        Using the tpcds.sql file generate the json files needed for the redis connector to interpret the
        redis hash object as a row in trino
        """
        sql_tables = self.create_sql_tables()
        for table in sql_tables:
            if table.name in self.TABLES_TO_EXCLUDE:
                continue
            table_name = table.name
            # TODO: Make sure if the key json is populated, the same col is removed from the other cols
            table_json = self.json_from_sqltable(table, include_keys)
            print(table_json)
            with open(f"{self.config.get_redis_json_dir()}{table_name}.json", "w+") as f:
                json.dump(table_json, f, indent=4)
            print(f"Successfully created json file for {table_name}")
    

    def create_sql_tables(self):
        """
        Returns a list of SQLTable objects from the tpcds.sql file
        This list can then be used for generating
        - CTAS statements
        - Insert Into statements
        - Drop Table statements
        """
        tables = []
        stmts = self.get_sql_table_to_stmt()
        for table_name, stmt in stmts.items():
            # print("-"*200)
            # print(table_name)
            # print(stmt)
            stmt = re.sub("--.*", "", stmt)
            # not null not supported
            stmt = stmt.replace("not null", "")
            # get the pkey
            pkey_line = re.search("(?<=primary key \().*(?=\))", stmt)
            pkey = None
            if pkey_line is not None:
                pkey = pkey_line.group(0)
                pkey = [i.strip() for i in pkey.split(",")]
                # print(pkey)
            stmt = re.sub(",[ \\n]*primary key.*", "", stmt)
            s_ = stmt.splitlines()
            cols = []
            for idx, line in enumerate(s_):
                if "create table" in line:
                    start_idx = idx+2
                    while(s_[start_idx].strip() != ")"):
                        cols.append(s_[start_idx])
                        start_idx += 1
                    break
            # print(cols)
            cols_final = {}
            for col in cols:
                col_name = col.strip().split()[0].strip()
                col_type = col.strip().split()[1].strip()
                cols_final[col_name] = col_type
            # print(cols_final)
            sql_table = SQLTable(name=table_name, cols=cols_final, pkey=pkey)
            tables.append(sql_table)
        # for table in tables:
        #     table.pprint()
        return tables
