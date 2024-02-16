import configparser


def load_sales(conn, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Reading Schema Names
        temp_schema = table_detail['SCHEMAS']['TEMP']
        target_schema = table_detail['SCHEMAS']['TARGET']

        # Reading Table Names
        temp_sales = table_detail['SALES']['TEMP']
        target_sales = table_detail['SALES']['TARGET']
        target_store = table_detail['STORE']['TARGET']
        target_product = table_detail['PRODUCT']['TARGET']
        target_customer = table_detail['CUSTOMER']['TARGET']

        cur = conn.cursor()
        cur.execute(f'USE DATABASE {database}')
        cur.execute(f"""
            INSERT INTO {target_schema}.{target_sales} ( id, store_key, product_key, customer_key, transaction_time, quantity, amount, discount)
            SELECT 
                sales.id,
                store.store_key,
                product.product_key,
                customer.customer_key,
                transaction_time,
                quantity,
                amount,
                NVL(discount, 0)
            FROM {temp_schema}.{temp_sales} sales
            LEFT JOIN {target_schema}.{target_store} store
                ON sales.store_id = store.id
            LEFT JOIN {target_schema}.{target_product} product
                ON sales.product_id = product.id
            LEFT JOIN {target_schema}.{target_customer} customer
                ON sales.customer_id = customer.id;
        """)

        # Loading aggregate table
        cur.execute(f"""
            CREATE OR REPLACE TABLE {target_schema}.DWH_F_AGG_SLS_PLC_MONTH_T AS
            (SELECT 
                store_id,
                monthname(transaction_time) as month,
                SUM(amount) as amount,
                SUM(NVL(discount, 0)) as discount
            FROM {temp_schema}.{temp_sales}
            GROUP BY store_id, month);
        """)

        print(f'Loaded {target_schema}.{target_sales} from {temp_schema}.{temp_sales}.')
    except Exception as e:
        print(f'ETL process failed for Sales with error {e}')