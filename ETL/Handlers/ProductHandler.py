import configparser


def load_product(conn, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Reading Schema and Table Names form the INI file created .....
        temp_schema = table_detail['SCHEMAS']['TEMP']
        temp_product = table_detail['PRODUCT']['TEMP']

        target_schema = table_detail['SCHEMAS']['TARGET']
        target_product = table_detail['PRODUCT']['TARGET']
        target_subcategory = table_detail['SUBCATEGORY']['TARGET']

        cur = conn.cursor()
        cur.execute(f"USE DATABASE {database}")

        cur.execute(f"""
                INSERT INTO {target_schema}.{target_product} (id, sub_category_key, product_desc)
                SELECT r.id, sub_category_key, product_desc
                FROM {temp_schema}.{temp_product} r
                JOIN {target_schema}.{target_subcategory} c
                ON r.subcategory_id = c.id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {target_schema}.{target_product} dest
                    WHERE dest.id = r.id
                );
            """)
        # Record once removed added to the source again
        cur.execute(f"""
                UPDATE {target_schema}.{target_product} dest
                SET active_flag = TRUE,
                CREATED_TS = CURRENT_TIMESTAMP(),
                UPDATED_TS = TO_TIMESTAMP('2999-01-01 00:00:00.00')
                WHERE EXISTS (
                    SELECT 1
                    FROM {temp_schema}.{temp_product} src
                    WHERE dest.id = src.id AND dest.active_flag = FALSE
                );
            """)

        # Record removed from source
        cur.execute(f"""
                UPDATE {target_schema}.{target_product} dest
                SET active_flag = FALSE,
                UPDATED_TS = CURRENT_TIMESTAMP()
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {temp_schema}.{temp_product} src
                    WHERE src.id = dest.id
                );
            """)

        # Minor change
        cur.execute(f"""
                UPDATE {target_schema}.{target_product} dest
                SET product_desc = src.product_desc
                FROM {temp_schema}.{temp_product} src
                WHERE dest.id = src.id
                AND dest.product_desc != src.product_desc;
            """)
        # Major change
        cur.execute(f"""
                UPDATE {target_schema}.{target_product} dest
                SET active_flag = FALSE,
                UPDATED_TS = CURRENT_TIMESTAMP()
                WHERE EXISTS (
                    SELECT 1
                    FROM (
                        SELECT product.id, product_desc, sub_category_key
                        FROM {temp_schema}.{temp_product} product
                        JOIN {target_schema}.{target_subcategory} subcategory
                        ON product.subcategory_id = subcategory.id
                    ) src
                    WHERE dest.id = src.id AND dest.sub_category_key != src.sub_category_key
                );
            """)
        print(f'Loaded {target_schema}.{target_product} from {temp_schema}.{temp_product}')
        return

    except Exception as e:
        print(f'ETL process failed for Product:with error {e}')
