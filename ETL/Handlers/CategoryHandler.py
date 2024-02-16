import configparser


def load_category(session, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Reading Schema and Table Names form the INI file created .....
        temp_schema = table_detail['SCHEMAS']['TEMP']
        target_schema = table_detail['SCHEMAS']['TARGET']
        temp_category = table_detail['CATEGORY']['TEMP']
        target_category = table_detail['CATEGORY']['TARGET']

        cursor = session.cursor()

        cursor.execute(f"USE DATABASE {database};")
        cursor.execute(f"""
            INSERT INTO {target_schema}.{target_category} (id, category_desc)
            SELECT id, category_desc
            FROM {temp_schema}.{temp_category} src
            WHERE NOT EXISTS (
                SELECT 1
                FROM {target_schema}.{target_category} dest
                WHERE dest.id = src.id);"""
        )

        # Record once removed added to the source again
        cursor.execute(f"""
            UPDATE {target_schema}.{target_category} dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('2999-01-01 00:00:00.00')
            WHERE EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_category} src
                WHERE dest.id = src.id AND dest.active_flag = FALSE
            );"""
        )

        # Record removed from source
        cursor.execute(f"""
            UPDATE {target_schema}.{target_category} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_category} src
                WHERE src.id = dest.id
            );"""
        )

        # Minor change
        cursor.execute(f"""
            UPDATE {target_schema}.{target_category} dest
            SET category_desc = src.category_desc
            FROM {temp_schema}.{temp_category} src
            WHERE dest.id = src.id
            AND dest.category_desc != src.category_desc;
        """)

        print(f'Loaded {target_schema}.{target_category} to {target_schema}.{target_category}')
    except Exception as e:
        print(f"Failed with Error: {e}")
