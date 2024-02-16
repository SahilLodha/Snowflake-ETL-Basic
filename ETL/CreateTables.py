# ----------------------------------------------------
# Base Imports
import csv
import os.path

# ----------------------------------------------------
# Script Files
from ETL.DDL_Scripts.target import TGT_DWH
from ETL.DDL_Scripts.staging import STG_DWH


class TableMethods:
    def __init__(self, session, database):
        self.session = session
        self.database = database

    def create_database(self):
        cursor = self.session.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        print("Database Created")
        cursor.close()

    def generate(self):
        self.create_database()
        self.generate_stage('STG_DWH')
        self.generate_temp('STG_DWH', 'TMP_DWH')
        self.generate_target('DW_DWH')

    def generate_temp(self, stage_schema, temp_schema):
        cursor = self.session.cursor()
        self.drop_schema(self.database, temp_schema)
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {self.database}.{temp_schema}')

        print(f"{'-' * 50}\nGenerating Temp Tables: \n{'-' * 50}")
        for key in STG_DWH.keys():
            print("Creating Table {0}.{1}.{2}".format(self.database, temp_schema, 'TMP' + key[3:]), end=' - ')
            cursor.execute(
                f'create or replace table {self.database}.{temp_schema}.TMP{key[3:]} LIKE {self.database}.{stage_schema}.{key};')
            print("Completed")

        print("DDL Process Completed for Temp Tables.")
        cursor.close()

    def generate_stage(self, schema):
        cursor = self.session.cursor()
        self.drop_schema(self.database, schema)
        cursor.execute("create schema {1}.{0}".format(schema, self.database))

        print(f"{'-' * 50}\nGenerating Stage Tables: \n{'-' * 50}")
        for key, value in STG_DWH.items():
            print("Creating Table {0}.{1}.{2}".format(self.database, schema, key), end=' - ')
            cursor.execute(value.format(self.database, schema, key))
            print("Completed")

        print("DDL Process Completed for Stage Tables.")
        cursor.close()

    def generate_target(self, schema):
        cursor = self.session.cursor()
        self.drop_schema(self.database, schema)
        cursor.execute("Create Schema {1}.{0}".format(schema, self.database))

        print(f"{'-' * 50}\nGenerating Target Tables: \n{'-' * 50}")
        for key, value in TGT_DWH.items():
            print("Creating Table {0}.{1}.{2}".format(self.database, schema, key), end=' - ')
            cursor.execute(value.format(self.database, schema, key))
            print("Completed")

        print("DDL Process Completed for Target Tables.")
        cursor.close()

    def drop_schema(self, database, schema):
        query = 'DROP SCHEMA IF EXISTS'
        cursor = self.session.cursor()
        cursor.execute(query + f' {database}.{schema}')
        cursor.close()

    def drop_table(self, table: list | str | tuple, database, schema):
        if isinstance(table, str):
            cursor = self.session.cursor()
            query = 'DROP TABLE IF EXISTS ' + f'{database}.{schema}.{table}'
            print(f"Dropping Table {database}.{schema}.{table} - Success.")
            cursor.execute(query)
            cursor.close()
        else:
            for element in table:
                self.drop_table(element, database, schema)

    def export_tables_from_schema(self, schema, folder='csv', database=None):
        if not database or database is None:
            database = self.database

        cursor = self.session.cursor()
        # Getting Tables from Database.
        cursor.execute(f"""select TABLE_NAME 
            FROM {database}.INFORMATION_SCHEMA.TABLES
            where TABLE_SCHEMA='{schema}'
        """)

        # getting table names from location ...
        table_names = [str(item[0]) for item in cursor.fetchall()]

        print(f"{'-'*50}\nDumping tables from: {database.upper()}.{schema.upper()}\n{'-'*50}")
        # Generating folder if not exists ...
        dump_loc = os.path.join(os.getcwd(), folder)
        if not os.path.exists(dump_loc):
            os.makedirs(dump_loc)

        for table in table_names:
            cursor.execute(f'select * from {database}.{schema}.{table}')
            rows = cursor.fetchall()
            col_name = [i[0] for i in cursor.description]
            with open(os.path.join(dump_loc, f'{table}.csv'), 'w') as f:
                writer = csv.writer(f, lineterminator='\n')
                writer.writerow(col_name)
                writer.writerows(rows)
                print(f"{database}.{schema}.{table} exported to {dump_loc}.")
