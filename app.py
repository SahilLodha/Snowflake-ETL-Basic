from ETL.Staging import Staging
from ETL.CreateTables import TableMethods
from ETL.utils.SnowflakeConnection import Connection

from ETL.Handlers.StoreHandler import load_store
from ETL.Handlers.SalesHandler import load_sales
from ETL.Handlers.RegionHandler import load_region
from ETL.Handlers.CountryHandler import load_country
from ETL.Handlers.ProductHandler import load_product
from ETL.Handlers.CategoryHandler import load_category
from ETL.Handlers.CustomerHandler import load_customer
from ETL.Handlers.SubcategoryHandler import load_subcategory

if __name__ == "__main__":
    connection = Connection(path='./.config.ini')
    with connection.connect() as session:
        table = TableMethods(session, 'LIS_SAHILLODHA_DW')
        table.export_tables_from_schema('TRANSACTIONS', database='BHATBHATENI', folder='dumps')
        # Creates all Database Tables
        table.generate()

        staging = Staging(session, 'LIS_SAHILLODHA_DW')
        staging.push_files_to_stage('STG_DWH', 'dumps')
        staging.load_stage_from_file()
        staging.copy_to_temp()

        print(f"{'-' * 50}\nLoading Target Tables\n{'-' * 50}")
        # Hierarchy Bases Function Calling
        load_country(session, 'LIS_SAHILLODHA_DW')
        load_region(session, 'LIS_SAHILLODHA_DW')
        load_store(session, 'LIS_SAHILLODHA_DW')

        load_customer(session, 'LIS_SAHILLODHA_DW')

        # Hierarchy based calling ....
        load_category(session, 'LIS_SAHILLODHA_DW')
        load_subcategory(session, 'LIS_SAHILLODHA_DW')
        load_product(session, 'LIS_SAHILLODHA_DW')

        # Loading the Fact Table using all above Tables + Aggregation Table has also beem loaded
        load_sales(session, 'LIS_SAHILLODHA_DW')
