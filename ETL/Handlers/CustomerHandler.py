import configparser


def load_customer(conn, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Reading Schema and Table Names form the INI file created .....
        temp_schema = table_detail['SCHEMAS']['TEMP']
        target_schema = table_detail['SCHEMAS']['TARGET']
        temp_customer = table_detail['CUSTOMER']['TEMP']
        target_customer = table_detail['CUSTOMER']['TARGET']
    
        cur = conn.cursor()
        
        cur.execute(f"USE DATABASE {database}")
        
        # New record added
        cur.execute(f"""
            INSERT INTO {target_schema}.{target_customer} (id, customer_first_name, customer_middle_name, customer_last_name, customer_address)
            SELECT id, customer_first_name, customer_middle_name, customer_last_name, customer_address
            FROM {temp_schema}.{temp_customer} src
            WHERE NOT EXISTS (
                SELECT 1
                FROM {target_schema}.{target_customer} dest
                WHERE dest.id = src.id
            );
         """)

        # Record once removed added to the source again
        cur.execute(f"""
            UPDATE {target_schema}.{target_customer} dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('2999-01-01 00:00:00.00')
            WHERE EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_customer} src
                WHERE dest.id = src.id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute(f"""
            UPDATE {target_schema}.{target_customer} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_customer} src
                WHERE src.id = dest.id
            );
        """)

        # Minor changes
        cur.execute(f"""
            UPDATE {target_schema}.{target_customer} dest
            SET customer_first_name = src.customer_first_name,
            customer_last_name = src.customer_last_name,
            customer_middle_name = src.customer_middle_name,
            customer_address = src.customer_address
            FROM {temp_schema}.{temp_customer} src
            WHERE dest.id = src.id
            AND (dest.customer_first_name != src.customer_first_name
            OR dest.customer_last_name != src.customer_last_name
            OR dest.customer_middle_name != src.customer_middle_name
            OR dest.customer_address != src.customer_address);
        """)
        print(f'Loaded {temp_schema}.{temp_customer} to {target_schema}.{target_customer}.')


    except Exception as e:
        print(f'ETL process failed for CUSTOMERwith error {e}')