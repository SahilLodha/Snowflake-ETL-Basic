TGT_DWH = {
    'DWH_D_STORE_RGN_CNT_LU': """create or replace table {0}.{1}.{2}(
        country_key NUMBER NOT NULL AUTOINCREMENT,
        id NUMBER,
        country_desc VARCHAR(256),
        active_flag Boolean DEFAULT TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        PRIMARY KEY (country_key)
    );""",

    'DWH_D_STORE_RGN_LU': """create or replace table {0}.{1}.{2}(
        region_key NUMBER NOT NULL AUTOINCREMENT,
        id NUMBER,
        country_key NUMBER,
        region_desc VARCHAR(256),
        active_flag Boolean DEFAULT TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        PRIMARY KEY (region_key),
        FOREIGN KEY (country_key) references DWH_D_STORE_RGN_CNT_LU(country_key) 
    );""",

    'DWH_D_STORE_LU': """create or replace table {0}.{1}.{2} (
        store_key NUMBER NOT NULL AUTOINCREMENT,
        id NUMBER,
        region_key NUMBER,
        store_desc VARCHAR(256),
        active_flag Boolean DEFAULT TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        PRIMARY KEY (store_key),
        FOREIGN KEY (region_key) references DWH_D_STORE_RGN_LU(region_key) 
    );""",

    'DWH_D_PROD_SCAT_CAT_LU': """create or replace table {0}.{1}.{2} (
        category_key NUMBER NOT NULL AUTOINCREMENT,
        id NUMBER,
        category_desc VARCHAR(1024),
        active_flag Boolean DEFAULT TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        PRIMARY KEY (category_key)
    );""",

    'DWH_D_PROD_SCAT_LU': """create or replace table {0}.{1}.{2}(
        sub_category_key NUMBER NOT NULL AUTOINCREMENT,
        id NUMBER,
        category_key NUMBER,
        subcategory_desc VARCHAR(256),
        active_flag Boolean DEFAULT TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        PRIMARY KEY (sub_category_key),
        FOREIGN KEY (category_key) references DWH_D_PROD_SCAT_CAT_LU(category_key)
    );""",

    'DWH_D_PROD_LU': """create or replace table {0}.{1}.{2} (
        product_key NUMBER NOT NULL AUTOINCREMENT,
        id NUMBER,
        sub_category_key NUMBER,
        product_desc VARCHAR(256),
        active_flag Boolean DEFAULT TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        PRIMARY KEY (product_key),
        FOREIGN KEY (sub_category_key) references DWH_D_PROD_SCAT_LU(sub_category_key)
    );""",

    'DWH_D_CUSTOMER_LU': """create or replace table {0}.{1}.{2} (
        customer_key  NUMBER NOT NULL AUTOINCREMENT,
        id NUMBER,
        customer_first_name VARCHAR(256),
        customer_middle_name VARCHAR(256),
        customer_last_name VARCHAR(256),
        customer_address VARCHAR(256) ,
        active_flag Boolean DEFAULT TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        primary key (customer_key)
    );""",

    'DWH_F_SALES_DTL': """create or replace table {0}.{1}.{2} (
        sales_key NUMBER AUTOINCREMENT,
        id NUMBER,
        store_key NUMBER,
        product_key NUMBER,
        customer_key NUMBER,
        transaction_time TIMESTAMP,
        quantity NUMBER,
        amount NUMBER(20,2),
        discount NUMBER(20,2),
        active_flag Boolean default TRUE,
        created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_ts TIMESTAMP DEFAULT '2099-01-01'::date,
        primary key (sales_key),
        FOREIGN KEY (store_key) references DWH_D_STORE_LU(store_key),
        FOREIGN KEY (product_key) references DWH_D_PROD_LU(product_key),
        FOREIGN KEY (customer_key) references DWH_D_CUSTOMER_LU(customer_key)
    );"""
}
