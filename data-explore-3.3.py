#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Winnie补充内容：
1、查询语句加上了date=current_date::text的限制，对当天的数据进行数据探查
2、将空值率改成了非空率和文档进行对应
3、gp查询的表可以输入表名用逗号分割，来对自己需要的表进行数据探查。
4、可以添加你想保存结果的文件的名称以及已探查表名的保存文件
5、输入过滤条件来查询自己需要的内容：and date=CURRENT_DATE::text
"""

"""
This is a data explore script, result will be saved in a explore_result.csv file in current path.Header is :
Host, Port, Database, Schema, Table/View Name, Table/View Type, Column Name, Column Type,Column Comment,Table/View Lines,Null Count,Unique Count

Unique Count will executed by below SQL statement, it can be edited if needed.
SELECT {column_name}, COUNT(1) FROM (select {column_name} from {table_name}  limit 1000000) t GROUP BY  {column_name} LIMIT 100

author: 
email: 
version：3.3
支持Oracle
"""
import os
import pandas as pd
import numpy as np
import cx_Oracle
import pymysql
import psycopg2
import json
import datetime
from datetime import date


class DataExplorer:
    def __init__(self):
        self.db_type = None
        self.db_host = None
        self.db_port = None
        self.db_user = None
        self.db_password = None
        self.connection = None
        self.cursor = None
        self.db_name = None
        self.schemas = None
        self.tables = None
        self.explored_tablelist = []
        self.selected_schemas = {}


    def date_handler(obj):
        if isinstance(obj, date):
            return obj.isoformat()

    def current_time(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 选择一个数据库类型
    def select_db_type(self) -> None:
        print("Please select a database type:")
        print("1. Oracle")
        print("2. MySQL")
        print("3. Greenplum")
        self.db_type = input("Enter your choice (1/2/3): ").strip()
        if self.db_type == "1":
            self.db_type = "oracle"
            cx_Oracle.init_oracle_client(lib_dir="/Users/wb.xionghaoqiang01/Downloads/instantclient_19_8",
                                         config_dir="/Users/wb.xionghaoqiang01/oracle/your_config_dir")
        elif self.db_type == "2":
            self.db_type = "mysql"
        elif self.db_type == "3":
            self.db_type = "greenplum"
        else:
            raise ValueError("Invalid database type.")

    # 选择一个数据库类型
    def ingore_or_update(self) -> None:
        print("If table already explored, Please select Skip or Re-expolore:")
        print("1. Skip")
        print("2. Re-expolore")
        self.iou = input("Enter your choice (1/2): ").strip()
        if self.iou == "1":
            self.iou = "Skip"
        elif self.iou == "2":
            self.iou = "Re-expolore"
        else:
            raise ValueError("Invalid selection.")

    # 终端输入数据库连接信息
    def get_db_info(self) -> None:
        if self.db_type == "oracle":
            self.db_host = input("Enter the database host: ").strip()
            self.db_port = input("Enter the database port: ").strip()
            self.db_user = input("Enter the database user: ").strip()
            self.db_password = input("Enter the database password: ").strip()
        elif self.db_type == "mysql":
            self.db_host = input("Enter the database host: ").strip()
            self.db_port = input("Enter the database port: ").strip()
            self.db_user = input("Enter the database user: ").strip()
            self.db_password = input("Enter the database password: ").strip()
        elif self.db_type == "greenplum":
            self.db_host = input("Enter the database host: ").strip()
            self.db_port = input("Enter the database port: ").strip()
            self.db_user = input("Enter the database user: ").strip()
            self.db_password = input("Enter the database password: ").strip()
        else:
            raise ValueError("Invalid database type.")

    # 连接到database
    def connect_to_database(self, database=None) -> None:
        if self.db_type == "oracle":
            try:
                self.connection = cx_Oracle.connect(
                    f"{self.db_user}/{self.db_password}@{self.db_host}:{self.db_port}"
                )
                self.cursor = self.connection.cursor()
            except Exception as e:
                current_time = self.current_time()
                print(f"{current_time} Error connecting to Oracle database: {e}")
        elif self.db_type == "mysql":
            try:
                self.connection = pymysql.connect(
                    host=self.db_host,
                    port=int(self.db_port),
                    user=self.db_user,
                    password=self.db_password,
                    database=database,
                )
                self.cursor = self.connection.cursor()
            except Exception as e:
                current_time = self.current_time()
                print(f"{current_time} Error connecting to MySQL database: {e}")
        elif self.db_type == "greenplum":
            try:
                self.connection = psycopg2.connect(
                    host=self.db_host,
                    port=int(self.db_port),
                    user=self.db_user,
                    password=self.db_password,
                    database=database,
                )
                self.cursor = self.connection.cursor()
            except Exception as e:
                current_time = self.current_time()
                print(f"{current_time} Error connecting to Greenplum database: {e}")
        else:
            raise ValueError("Invalid database type.")

    # 获取dblist
    def get_db_list(self) -> None:
        if self.db_type == "oracle":
            self.cursor.execute(f"SELECT name FROM v$database;")
            self.tables = [row[0] for row in self.cursor.fetchall()]
        elif self.db_type == "mysql":
            self.cursor.execute(f"SHOW DATABASES WHERE `Database` NOT like ('%schema%');")
            self.tables = [row[0] for row in self.cursor.fetchall()]
        elif self.db_type == "greenplum":
            self.cursor.execute(f"SELECT datname FROM pg_database WHERE datistemplate = false")
            self.tables = [row[0] for row in self.cursor.fetchall()]

    # 获取所有database
    def select_dbs(self) -> None:
        print("Please select one or more databases to query:")
        for i, database in enumerate(self.tables):
            print(f"{i + 1}. {database}")
        print(f"{len(self.tables) + 1}. All databases")
        # 让用户选择database
        database_selection = input(
            "Enter the number(s) of the databases you want to query, separated by commas: ").strip()
        if database_selection == str(len(self.tables) + 1):
            self.selected_databases = self.tables
        else:
            self.selected_databases = [self.tables[int(i) - 1] for i in database_selection.split(",")]

    # 获取schemalist
    def get_schema_list(self) -> None:
        if self.db_type == "mysql":
            pass
        else:
            for database in self.selected_databases:
                if self.db_type == "oracle":
                    self.cursor.execute(f"SELECT DISTINCT OWNER FROM ALL_TABLES ORDER BY OWNER;")
                    schemas = [row[0] for row in self.cursor.fetchall()]
                elif self.db_type == "greenplum":
                    self.connect_to_database(database)
                    self.cursor.execute(f"SELECT schema_name \
                                        FROM information_schema.schemata \
                                        WHERE catalog_name = '{database}' and schema_name not like 'gp_%' and schema_name not like 'pg_%' and schema_name  not in ( 'information_schema')")
                    schemas = [row[0] for row in self.cursor.fetchall()]
                print(f"Please select one or more schemas with database {database} to query:")
                for i, schema in enumerate(schemas):
                    print(f"{i + 1}. {schema}")
                print(f"{len(schemas) + 1}. All schemas")
                # 让用户选择database
                schemma_selection = input(
                    "Enter the number(s) of the schemas you want to query, separated by commas: ").strip()
                if schemma_selection == str(len(schemas) + 1):
                    selected_schemas = schemas
                else:
                    selected_schemas = [schemas[int(i) - 1] for i in schemma_selection.split(",")]
                self.selected_schemas[database] = selected_schemas

    # 探索全部的数据库，定义初始文件，以及header
    #输入探查结果的保存文件命名--Winnie
    def explore_databases(self) -> None:
        self.explore_result_file = f"explore_result.csv"
        self.explore_result_file = input('input file name you want to save the results, for example xxx.csv:')
        if os.path.exists(self.explore_result_file):
            pass
        else:
            columns = ["DB Type", "Host", "Port", "Database", "Schema", "Table/View Name", "Table/View Type",
                       "Column Name", "Column Type", "Column Comment", "Table/View Lines", "Null Count", "NOT Null Ratio",
                       "Data Example","Unique Count"]
            df = pd.DataFrame(columns=columns)
            df.to_csv(self.explore_result_file, index=False, header=True)
        self.explored_tables_file = input('input file name you want to save the results table, for example table.csv:')
        if os.path.exists(self.explored_tables_file):
            df = pd.read_csv(self.explored_tables_file, header=None, na_filter=False).apply(tuple, axis=1)
            self.explored_tablelist = df.values.tolist()
        else:
            columns = ["DB type", "Host", "Port", "Database", "Schema", "Table/View Name", "Table/View Type"]
            df = pd.DataFrame(columns=columns)
            df.to_csv(self.explored_tables_file, index=False, header=True)
        for db in self.selected_databases:
            self.explore_one_database(db)

    def explore_one_database(self, db: str) -> None:
        current_time = self.current_time()
        # 查询出一个DB下全部的表和视图
        try:
            if self.db_type == "oracle":
                schemas = self.selected_schemas[db]
                schemas_print = ','.join(schemas)
                schemas_sql = ','.join(["'" + item + "'" for item in schemas])
                print(f"{current_time} Exploring database: {db}, schemas: {schemas_print} ...")
                self.connect_to_database(db)
                self.cursor.execute(f"SELECT * FROM ( \
                                    SELECT '{self.db_type}' as DB_type, '{self.db_host}' as host , '{self.db_port}' as port,{db} as database_name, OWNER as schema_name,TABLE_NAME,'TABLE' TABLE_TYPE  FROM ALL_TABLES  UNION ALL \
                                    SELECT '{self.db_type}' as DB_type, '{self.db_host}' as host , '{self.db_port}' as port,{db} as database_name, OWNER as schema_name,VIEW_NAME,'VIEW' TABLE_TYPE FROM ALL_VIEWS) t \
                                    WHERE OWNER='{db}';")
                # 获取到一个database下全部的表和视图，表和视图不会重名
                tables = self.cursor.fetchall()
                table_number = len(tables)
                tables = self.cursor.fetchall()
            elif self.db_type == "mysql":
                print(f"{current_time} Exploring database: {db} ...")
                self.connect_to_database(db)
                self.cursor.execute(f"SHOW full TABLES LIKE 'dm%';")
                tables = self.cursor.fetchall()
                tables = tuple((self.db_type, self.db_host, self.db_port, db, '', *tup) for tup in tables)
                table_number = len(tables)
                #添加了selected_tables，用户可以自己选择具体查询那些表--Winnie
            elif self.db_type == "greenplum":
                tables_str = input("input tables which you want to explore and split by ',':").strip()
                dt = input("input the filter conditions:").strip()
                selected_tables = tables_str.split(",")
                tables_sql = ','.join(["'" + item + "'" for item in selected_tables])
                schemas = self.selected_schemas[db]
                schemas_print = ','.join(schemas)
                schemas_sql = ','.join(["'" + item + "'" for item in schemas])
                print(f"{current_time} Exploring database: {db}, schemas: {schemas_print} ...")
                self.connect_to_database(db)
                self.cursor.execute(
                    f"SELECT '{self.db_type}' as DB_type, '{self.db_host}' as host , '{self.db_port}' as port, '{db}' as database_name, nspname AS schema_name, relname AS table_name, \
                                    case when relkind = 'r' then 'Table' when relkind = 'v' then 'View' end as table_type \
                                    FROM pg_class c \
                                    INNER JOIN pg_namespace n ON c.relnamespace = n.oid \
                                    WHERE relkind in('v','r') and nspname NOT LIKE 'pg_%' and nspname not like 'gp_%' AND nspname <> 'information_schema' and nspname in ({schemas_sql}) and relname in ({tables_sql}) order by 1,2,3,4")
                # 获取到一个database下全部的表和视图，表和视图不会重名
                tables = self.cursor.fetchall()
                table_number = len(tables)
        except Exception as e:
            current_time = self.current_time()
            print(f"{current_time} Error exploring database {db}: {e}".format(db, e))
            exit(1)
        # 开始处理每个表
        for i, table in enumerate(tables):
            try:
                # 对表名进行拆解
                if self.db_type == "oracle":
                    schema_name = table[4]
                    table_name = table[5]
                    table_type = table[6]
                    table_link = table[:7]
                    table_log = f"{db}.{table_name}"
                    if table_link in self.explored_tablelist and self.iou == "Skip":
                        current_time = self.current_time()
                        print(f"{current_time} Exploring table/view {table_log} skip {i + 1}/{table_number} ...")
                        continue
                    else:
                        current_time = self.current_time()
                        print(f"{current_time} Exploring table/view {table_log} exploring {i + 1}/{table_number} ...")
                elif self.db_type == "mysql":
                    schema_name = table[4]
                    table_name = table[5]
                    table_type = table[6]
                    table_link = table[:7]
                    table_log = f"{db}.{table_name}"
                    if table_link in self.explored_tablelist and self.iou == "Skip":
                        current_time = self.current_time()
                        print(f"{current_time} Exploring table/view {table_log} skip {i + 1}/{table_number} ...")
                        continue
                    else:
                        current_time = self.current_time()
                        print(f"{current_time} Exploring table/view {table_log} exploring {i + 1}/{table_number} ...")
                elif self.db_type == "greenplum":
                    # Greenplum表名大小写敏感，因此用双引号包裹
                    table_name_SQL = '"{}"."{}"."{}"'.format(table[3], table[4], table[5])
                    schema_name = table[4]
                    table_name = table[5]
                    table_type = table[6]
                    table_link = table[:7]
                    table_log = '.'.join(table[3:6])
                    if table_link in self.explored_tablelist and self.iou == "Skip":
                        current_time = self.current_time()
                        print(f"{current_time} Table/view {table_log} Skip {i + 1}/{table_number} ...")
                        continue
                    else:
                        current_time = self.current_time()
                        print(f"{current_time} Table/view {table_log} Exploring {i + 1}/{table_number} ...")
                # 查询表的字段名、字段类型、字段注释，然后对每个字段进行分析
                if self.db_type == "oracle":
                    # 查询字段备注信息
                    self.connect_to_database(db)
                    statement = f"SELECT COLUMN_NAME,COMMENTS  FROM ALL_COL_COMMENTS WHERE OWNER='{db}' AND TABLE_NAME ='{table_name}'"
                    self.cursor.execute(statement)
                    columns = self.cursor.fetchall()
                    column_names = [column[0] for column in columns]
                    column_types = [column[1] for column in columns]
                    column_comments = [column[2] for column in columns]
                    # 获取表行数
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table_name_SQL}")
                    row_count = self.cursor.fetchone()[0]
                    column_null_counts = []
                    column_null_ratio = []
                    column_uniq_counts = []
                    # 对字段逐个分析
                    for column_name in column_names:
                        # 统计NULL数量
                        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name_SQL} WHERE \"{column_name}\" IS NULL")
                        null_count = self.cursor.fetchone()[0]
                        column_null_counts.append(null_count)
                        if row_count == 0:
                            null_ratio = ''
                        else:
                            null_ratio = null_count / row_count
                        column_null_ratio.append(null_ratio)
                        # 统计每个字段的值的数量，为避免信息过多，比如ID字段等，在Group by之后只取100行，应该足够辨别是物理枚举，还是业务枚举（业务枚举是指数据类型不是枚举，但业务输入数据时可能是下拉列表等业务上的枚举）
                        self.cursor.execute(
                            f"SELECT \"{column_name}\", COUNT(1) FROM (select \"{column_name}\" from {table_name_SQL}  limit 1000000) t GROUP BY  \"{column_name}\" LIMIT 100")
                        rows = self.cursor.fetchall()
                        result = []
                        for row in rows:
                            obj = {}
                            if isinstance(row[0], date):
                                obj["value"] = row[0].isoformat()
                            else:
                                obj["value"] = row[0]
                            obj["count"] = row[1]
                            result.append(obj)
                        json_str = json.dumps(result, default=str, ensure_ascii=False)
                        column_uniq_counts.append(json_str)
                elif self.db_type == "mysql":
                    # 查询字段备注信息
                    self.connect_to_database(db)
                    statement = f"select COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT \
                                        from INFORMATION_SCHEMA.COLUMNS \
                                        where table_schema = '{db}' \
                                        and TABLE_NAME = '{table_name}';"
                    self.cursor.execute(statement)
                    columns = self.cursor.fetchall()
                    column_names = [column[0] for column in columns]
                    column_types = [column[1] for column in columns]
                    column_comments = [column[2] for column in columns]
                    # 获取表行数
                    self.cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                    row_count = self.cursor.fetchone()[0]
                    column_null_counts = []
                    column_null_ratio = []
                    column_uniq_counts = []
                    # 对字段逐个分析
                    for column_name in column_names:
                        # 统计NULL数量
                        statement = f"SELECT COUNT(*) FROM `{table_name}` WHERE `{column_name}` IS NULL"
                        self.cursor.execute(statement)
                        null_count = self.cursor.fetchone()[0]
                        column_null_counts.append(null_count)
                        if row_count == 0:
                            null_ratio = ''
                        else:
                            null_ratio = null_count / row_count
                        column_null_ratio.append(null_ratio)
                        # 统计每个字段的值的数量，为避免信息过多，比如ID字段等，在Group by之后只取100行，应该足够辨别是物理枚举，还是业务枚举（业务枚举是指数据类型不是枚举，但业务输入数据时可能是下拉列表等业务上的枚举）
                        statement = f"SELECT `{column_name}`, COUNT(1) FROM (select `{column_name}` from `{table_name}` limit 1000000) t GROUP BY `{column_name}` LIMIT 100"
                        self.cursor.execute(statement)
                        rows = self.cursor.fetchall()
                        result = []
                        for row in rows:
                            obj = {}
                            if isinstance(row[0], date):
                                obj["value"] = row[0].isoformat()
                            else:
                                obj["value"] = row[0]
                            obj["count"] = row[1]
                            result.append(obj)
                        json_str = json.dumps(result, default=str, ensure_ascii=False)
                        column_uniq_counts.append(json_str)
                elif self.db_type == "greenplum":
                    # 查询字段备注信息
                    self.connect_to_database(db)
                    statement = f"select a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS data_type, pgd.description AS column_comment \
                                        FROM pg_catalog.pg_attribute a \
                                        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid \
                                        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid \
                                        LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = c.oid AND pgd.objsubid = a.attnum \
                                        WHERE \
                                        attrelid = '{table_name_SQL}'::regclass \
                                        AND a.attnum > 0 \
                                        AND NOT a.attisdropped \
                                        ORDER BY a.attnum;"
                    self.cursor.execute(statement)
                    columns = self.cursor.fetchall()
                    column_names = [column[0] for column in columns]
                    column_types = [column[1] for column in columns]
                    column_comments = [column[2] for column in columns]
                    # 获取表行数
                    statement = f"SELECT COUNT(*) FROM {table_name_SQL} where 1=1 {dt}"
                    self.cursor.execute(statement)
                    row_count = self.cursor.fetchone()[0]
                    column_null_counts = []
                    column_null_ratio = []
                    column_uniq_counts = []
                    column_data_example = []
                    # 对字段逐个分析
                    for column_name in column_names:
                        # 统计NULL数量
                        statement = f"SELECT COUNT(*) FROM {table_name_SQL} where 1=1 and \"{column_name}\" IS NULL {dt}"
                        self.cursor.execute(statement)
                        null_count = self.cursor.fetchone()[0]
                        column_null_counts.append(null_count)
                        if row_count == 0:
                            null_ratio = ''
                        else:
                            null_ratio = (row_count-null_count) / row_count
                        column_null_ratio.append(null_ratio)
                        # 获取一条数据
                        statement = f"SELECT \"{column_name}\" FROM {table_name_SQL} where 1=1 {dt} limit 1"
                        self.cursor.execute(statement)
                        data_example = self.cursor.fetchone()[0]
                        column_data_example.append(data_example)
                        # 统计每个字段的值的数量，为避免信息过多，比如ID字段等，在Group by之后只取100行，应该足够辨别是物理枚举，还是业务枚举（业务枚举是指数据类型不是枚举，但业务输入数据时可能是下拉列表等业务上的枚举）
                        statement = f"SELECT \"{column_name}\", COUNT(1) FROM (select \"{column_name}\" from {table_name_SQL} where 1=1 {dt} limit 1000000) t GROUP BY  \"{column_name}\" LIMIT 100"
                        self.cursor.execute(statement)
                        rows = self.cursor.fetchall()
                        result = []
                        for row in rows:
                            obj = {}
                            if isinstance(row[0], date):
                                obj["value"] = row[0].isoformat()
                            else:
                                obj["value"] = row[0]
                            obj["count"] = row[1]
                            result.append(obj)
                        json_str = json.dumps(result, default=str, ensure_ascii=False)
                        column_uniq_counts.append(json_str)
                self.result = {
                    "DB Type": [self.db_type] * len(column_names),
                    "Host": [self.db_host] * len(column_names),
                    "Port": [self.db_port] * len(column_names),
                    "Database": [db] * len(column_names),
                    "Schema": [schema_name] * len(column_names),
                    "Table/View Name": [table_name] * len(column_names),
                    "Table/View Type": [table_type] * len(column_names),
                    "Column Name": column_names,
                    "Column Type": column_types,
                    "Column Comment": column_comments,
                    "Table/View Lines": [row_count] * len(column_names),
                    "Null Count": column_null_counts,
                    "NOT Null Ratio": column_null_ratio,
                    "Data Example": column_data_example,
                    "Unique Count": column_uniq_counts,
                }
                self.tablename = {
                    "DB Type": [self.db_type],
                    "Host": [self.db_host],
                    "Port": [self.db_port],
                    "Database": [db],
                    "Schema": [schema_name],
                    "Table/View Name": [table_name],
                    "Table/View Type": [table_type],
                }
                df = pd.DataFrame(self.result)
                df.to_csv(self.explore_result_file, index=False, encoding="utf-8", mode='a', header=False)
                df = pd.DataFrame(self.tablename)
                df.to_csv(self.explored_tables_file, index=False, encoding="utf-8", mode='a', header=False)
                current_time = self.current_time()
            except Exception as e:
                current_time = self.current_time()
                print(f"{current_time} Error exploring {table_log}. Exception: {e}")
                continue

    def run(self) -> None:
        self.select_db_type()
        self.get_db_info()
        self.connect_to_database()
        self.get_db_list()
        self.select_dbs()
        self.get_schema_list()
        self.ingore_or_update()
        self.explore_databases()


if __name__ == "__main__":
    print(__doc__)
    explorer = DataExplorer()
    explorer.run()
    print('数据探查结束!')
