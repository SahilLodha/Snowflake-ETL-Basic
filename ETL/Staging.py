import configparser
import os


class Staging:
    def __init__(self, session, database):
        self.session = session
        self.database = database

    def push_files_to_stage(self, schema, path='dumps'):
        try:
            file_dir = os.path.join(os.getcwd(), path)
            cursor = self.session.cursor()
            cursor.execute(f'USE SCHEMA {self.database}.{schema};')
            print(f"{'-' * 50}\nStaging Files from Local [Dump Generated Above]\n{'-' * 50}")
            # Creating a File stage
            cursor.execute(f'CREATE OR REPLACE STAGE FILE_STAGE;')
            for file in os.listdir(file_dir):
                print(f"Pushing file to staging: {file}")
                cursor.execute(f"""PUT file://{os.path.join(file_dir, file)} @FILE_STAGE;""")

            cursor.close()
        except Exception as e:
            print(e)

    def load_stage_from_file(self, schema='STG_DWH'):
        cursor = self.session.cursor()
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Setting Database and Schema Scopes
        cursor.execute(f'USE SCHEMA {self.database}.{schema};')

        print(f"{'-' * 50}\nLoading data from File stage to Stage Table\n{'-' * 50}")
        for section in table_detail.sections():
            if section != 'SCHEMAS':
                print(f"Truncating and re-loading {table_detail[section]['STAGE']} from {section}.csv.gz")
                cursor.execute(f'''TRUNCATE TABLE {table_detail[section]['STAGE']};''')
                cursor.execute(f'''Copy into {self.database}.{schema}.{table_detail[section]['STAGE']}
                    From @FILE_STAGE/{section}.csv.gz
                    file_format=(
                    type = csv,
                    field_delimiter = ',',
                    skip_header = 1)
                    on_error= "continue";
                ''')
        cursor.close()

    def copy_to_temp(self, temp_schema="TMP_DWH", stage_schema="STG_DWH"):
        cursor = self.session.cursor()
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        print(f"{'-'*50}\nLoading Temp Tables\n{'-'*50}")

        # Setting database ..
        cursor.execute(f'''USE DATABASE {self.database}''')

        for section in table_detail.sections():
            if section != 'SCHEMAS':
                temp_table_name = table_detail[section]['TEMP']
                src_table_name = table_detail[section]['STAGE']

                print(
                    f"Loading {temp_schema}.{temp_table_name} from {stage_schema}.{src_table_name}"
                )

                cursor.execute(f'''TRUNCATE TABLE {temp_schema}.{temp_table_name}''')
                cursor.execute(
                    f'''INSERT into {temp_schema}.{temp_table_name} SELECT * FROM {stage_schema}.{src_table_name}'''
                )

        cursor.close()
