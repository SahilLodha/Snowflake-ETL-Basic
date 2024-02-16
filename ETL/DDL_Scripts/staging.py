STG_DWH = {
    'STG_D_PROD_SCAT_CAT_LU': """create or replace table {0}.{1}.{2}(
        id NUMBER,
        category_desc VARCHAR(1024)
    );""",
    'STG_D_STORE_RGN_CNTY_LU': """create or replace table {0}.{1}.{2}
    (
        id NUMBER,
        country_desc VARCHAR(256)
    );""",
    'STG_D_STORE_RGN_LU': """create or replace table {0}.{1}.{2}(
        id NUMBER,
        country_id NUMBER,
        region_desc VARCHAR(256)
    );""",
    'STG_D_STORE_LU': """create or replace table {0}.{1}.{2}(
        id NUMBER,
        region_id NUMBER,
        store_desc VARCHAR(256)
    );""",
    'STG_D_PROD_SCAT_LU': """create or replace table {0}.{1}.{2}(
        id NUMBER,
        category_id NUMBER,
        subcategory_desc VARCHAR(256)
    );""",
    'STG_D_PROD_LU': """create or replace table {0}.{1}.{2}(
        id NUMBER,
        subcategory_id NUMBER,
        product_desc VARCHAR(256)
    );""",
    'STG_D_CUSTOMER_LU': """create or replace table {0}.{1}.{2}(
        id NUMBER,
        customer_first_name VARCHAR(256),
        customer_middle_name VARCHAR(256),
        customer_last_name VARCHAR(256),
        customer_address VARCHAR(256)
    );""",
    'STG_F_SALES_DTL': """create or replace table {0}.{1}.{2} (
        id NUMBER,
        store_id NUMBER NOT NULL,
        product_id NUMBER NOT NULL,
        customer_id NUMBER,
        transaction_time TIMESTAMP,
        quantity NUMBER,
        amount NUMBER(20,2),
        discount NUMBER(20,2)
    );"""
}
