import configparser


def load_subcategory(conn, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Reading Schema and Table Names form the INI file created .....
        temp_schema = table_detail['SCHEMAS']['TEMP']
        target_schema = table_detail['SCHEMAS']['TARGET']
        temp_category = table_detail['CATEGORY']['TEMP']
        temp_subcategory = table_detail['SUBCATEGORY']['TEMP']
        target_category = table_detail['CATEGORY']['TARGET']
        target_subcategory = table_detail['SUBCATEGORY']['TARGET']

        # Load Data to target
        cur = conn.cursor()
        cur.execute(f"USE DATABASE {database};")

        # New record added
        cur.execute(f"""
            INSERT INTO {target_schema}.{target_subcategory} (id, category_key, subcategory_desc)
            SELECT  r.id, category_key, r.subcategory_desc
            FROM {temp_schema}.{temp_subcategory} r
            JOIN {target_schema}.{target_subcategory} c
            ON r.category_id = c.id
            WHERE NOT EXISTS (
                SELECT 1
                FROM {target_schema}.{target_subcategory} dest
                WHERE dest.id = r.id
            );
        """)

        # Record once removed added to the source again
        cur.execute(f"""
            UPDATE {target_schema}.{target_subcategory} dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('2999-01-01 00:00:00.00')
            WHERE EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_subcategory} src
                WHERE dest.id = src.id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute(f"""
            UPDATE {target_schema}.{target_subcategory} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_subcategory} src
                WHERE src.id = dest.id
            );
        """)

        # Minor change
        cur.execute(f"""
            UPDATE {target_schema}.{target_subcategory} dest
            SET subcategory_desc = src.subcategory_desc
            FROM {temp_schema}.{temp_subcategory} src
            WHERE dest.id = src.id
            AND dest.subcategory_desc != src.subcategory_desc;
        """)

        # Major change
        cur.execute(f"""
            UPDATE {target_schema}.{target_subcategory} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE EXISTS (
                SELECT 1
                FROM (
                    SELECT subcategory.id, subcategory.subcategory_desc, category_key
                    FROM {temp_schema}.{temp_subcategory} subcategory
                    JOIN {target_schema}.{target_subcategory} category
                    ON subcategory.category_id = category.id
                ) src
                WHERE dest.id = src.id AND dest.category_key != src.category_key
            );
        """)

        cur.execute(f"""
            INSERT INTO {target_schema}.{target_subcategory}(id, subcategory_desc, category_key)
            SELECT id, subcategory_desc, category_key
            FROM (
                SELECT subcategory.id, subcategory.subcategory_desc, category_key
                FROM {temp_schema}.{temp_subcategory} subcategory
                JOIN {target_schema}.{target_subcategory} category
                ON subcategory.category_id = category.id
            ) src
            WHERE EXISTS (
                SELECT 1
                FROM {target_schema}.{target_subcategory} dest
                WHERE dest.id = src.id AND dest.category_key != src.category_key AND active_flag = TRUE
            );
        """)

        print(f'Loaded {target_schema}.{target_subcategory} from {temp_schema}.{temp_subcategory}.')

    except Exception as e:
        print(f'ETL process failed for Sub category with error {e}')
