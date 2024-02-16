import configparser


def load_country(conn, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Reading Schema and Table Names form the INI file created .....
        temp_schema = table_detail['SCHEMAS']['TEMP']
        target_schema = table_detail['SCHEMAS']['TARGET']
        temp_country = table_detail['COUNTRY']['TEMP']
        target_country = table_detail['COUNTRY']['TARGET']

        cur = conn.cursor()

        # Setting Database
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"""
            INSERT INTO {target_schema}.{target_country} (id, country_desc)
            SELECT id, country_desc
            FROM {temp_schema}.{temp_country} src
            WHERE NOT EXISTS (
                SELECT 1
                FROM {target_schema}.{target_country} dest
                WHERE dest.id = src.id
            );
         """)

        # Record once removed added to the source again
        cur.execute(f"""
            UPDATE {target_schema}.{target_country} dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('2999-01-01 00:00:00.00')
            WHERE EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_country} src
                WHERE dest.id = src.id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute(f"""
            UPDATE {target_schema}.{target_country} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_country} src
                WHERE src.id = dest.id
            );
        """)

        # Minor change
        cur.execute(f"""
            UPDATE {target_schema}.{target_country} dest
            SET country_desc = src.country_desc
            FROM {temp_schema}.{temp_country} src
            WHERE dest.id = src.id
            AND dest.country_desc != src.country_desc;
        """)

        print(f'Loaded {temp_schema}.{temp_country} to {target_schema}.{target_country}.')
    except Exception as e:
        print(f'ETL process failed for Countrywith error {e}')