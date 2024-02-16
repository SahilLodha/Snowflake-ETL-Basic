import configparser


def load_region(conn, database):
    try:
        table_detail = configparser.ConfigParser()
        table_detail.read('table.conf.ini')

        # Reading Schema and Table Names form the INI file created .....
        temp_schema = table_detail['SCHEMAS']['TEMP']
        target_schema = table_detail['SCHEMAS']['TARGET']

        # Table Details
        temp_region = table_detail['REGION']['TEMP']
        target_region = table_detail['REGION']['TARGET']
        target_country = table_detail['COUNTRY']['TARGET']

        cur = conn.cursor()
        # Database Selection ...
        cur.execute(f"USE DATABASE {database};")

        # Load Data to target
        cur.execute(f"""
            INSERT INTO {target_schema}.{target_region} (id, country_key, region_desc)
            SELECT r.id, country_key, region_desc
            FROM {temp_schema}.{temp_region} r
            JOIN {target_schema}.{target_country} c
            ON r.country_id = c.id
            WHERE NOT EXISTS (
                SELECT 1
                FROM {target_schema}.{target_region} dest
                WHERE dest.id = r.id
            );
        """)

        # Record once removed added to the source again
        cur.execute(f"""
            UPDATE {target_schema}.{target_region} dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('2999-01-01 00:00:00.00')
            WHERE EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_region} src
                WHERE dest.id = src.id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute(f"""
            UPDATE {target_schema}.{target_region} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {temp_schema}.{temp_region} src
                WHERE src.id = dest.id
            );
        """)

        # Minor change
        cur.execute(f"""
            UPDATE {target_schema}.{target_region} dest
            SET region_desc = src.region_desc
            FROM {temp_schema}.{temp_region} src
            WHERE dest.id = src.id
            AND dest.region_desc != src.region_desc;
        """)

        # Major change
        cur.execute(f"""
            UPDATE {target_schema}.{target_region} dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE EXISTS (
                SELECT 1
                FROM (
                    SELECT region.id, region_desc, country_key
                    FROM {temp_schema}.{temp_region} region
                    JOIN {target_schema}.{target_country} country
                    ON region.country_id = country.id
                ) src
                WHERE dest.id = src.id AND dest.country_key != src.country_key
            );
        """)

        cur.execute(f"""
            INSERT INTO {target_schema}.{target_region}(id, region_desc, country_key)
            SELECT id, region_desc, country_key
            FROM (
                SELECT region.id, region_desc, country_key
                FROM {temp_schema}.{temp_region} region
                JOIN {target_schema}.{target_country} country
                ON region.country_id = country.id
            ) src
            WHERE EXISTS (
                SELECT 1
                FROM {target_schema}.{target_region} dest
                WHERE dest.id = src.id AND dest.country_key != src.country_key AND active_flag = TRUE
            );
        """)

        print(f'Loaded {target_schema}.{target_region} from {temp_schema}.{temp_region}.')
    except Exception as e:
        print(f'ETL process failed for Regionwith error {e}')
