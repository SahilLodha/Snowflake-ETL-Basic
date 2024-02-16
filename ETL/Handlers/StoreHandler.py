import configparser


def load_store(conn, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')
        
        cur = conn.cursor()
        cur.execute(f'USE DATABASE {database}')
        
        # Loading Schema ...
        temp_schema = table_detail['SCHEMAS']['TEMP']
        target_schema = table_detail['SCHEMAS']['TARGET']
        # Loading Tables ...
        temp_store = table_detail['STORE']['TEMP']
        target_store = table_detail['STORE']['TARGET']
        target_region = table_detail['REGION']['TARGET']
        
        cur.execute(f"""
            INSERT INTO {target_schema}.{target_store} (id, region_key, store_desc)
            SELECT r.id, region_key, store_desc
            FROM {temp_schema}.{temp_store} r
            JOIN {target_schema}.{target_region} c
            ON r.region_id = c.id
            WHERE NOT EXISTS (
                SELECT 1
                FROM {target_schema}.{target_store} dest
                WHERE dest.id = r.id
            );
        """)

        # Record once removed added to the source again
        cur.execute(f"""
            UPDATE {target_schema}.{target_store} dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('2999-01-01 00:00:00.00')
            WHERE EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_store} src
                WHERE dest.id = src.id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute(f"""
            UPDATE {target_schema}.{target_store} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_store} src
                WHERE src.id = dest.id
            );
        """)

        # Minor change
        cur.execute(f"""
            UPDATE {target_schema}.{target_store} dest
            SET store_desc = src.store_desc
            FROM {temp_schema}.{temp_store} src
            WHERE dest.id = src.id
            AND dest.store_desc != src.store_desc;
        """)

        # Major change
        cur.execute(f"""
            UPDATE {target_schema}.{target_store} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE EXISTS (
                SELECT 1
                FROM (
                    SELECT store.id, store_desc, region_key
                    FROM {temp_schema}.{temp_store} store
                    JOIN {target_schema}.{target_region} region
                    ON store.region_id = region.id
                ) src
                WHERE dest.id = src.id AND dest.region_key != src.region_key
            );
        """)

        cur.execute(f"""
            INSERT INTO {target_schema}.{target_store}(id, store_desc, region_key)
            SELECT id, store_desc, region_key
            FROM (
                SELECT store.id, store_desc, region_key
                FROM {temp_schema}.{temp_store} store
                JOIN {target_schema}.{target_region} region
                ON store.region_id = region.id
            ) src
            WHERE EXISTS (
                SELECT 1
                FROM {target_schema}.{target_store} dest
                WHERE dest.id = src.id AND dest.region_key != src.region_key AND active_flag = TRUE
            );
        """)

        print(f'Loaded {target_schema}.{target_store} from {temp_schema}.{temp_store}.')

        return
    except Exception as e:
        print(f'ETL process failed for Storewith error {e}')