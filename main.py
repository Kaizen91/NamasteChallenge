import numpy as np
import pandas as pd
import json, requests
import sqlite3
from sqlite3 import Error
from pandas.io.json import json_normalize
import os

def create_connection(db_file):
    """
    Creates a db connection using sqlite
    :param db_file: the sqlite file being used
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def create_table(conn, create_table_sql):
    """
    Creates a sqlite table
    :param conn: a sqlite connection object
    :param create_table_sql: sql create table statement
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def insert_values_to_table(conn,table_name, df):
    """
    Inserts data into sqlite tables
    :param conn:  a sqlite connection object
    :param table_name:  the table name
    :param df: a pandas dataframe
    """

    if conn is not None:
        c = conn.cursor()

        df.columns = get_column_names_from_db_table(c, table_name)

        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

        print('SQL insert process finished')
    else:
        print('Connection to database failed')



def get_column_names_from_db_table(sql_cursor, table_name):
    """
    Scrape the column names from a database table to a list
    :param sql_cursor: sqlite cursor
    :param table_name: table name to get the column names from
    :return: a list with table column names
    """

    table_column_names = 'PRAGMA table_info(' + table_name + ');'
    sql_cursor.execute(table_column_names)
    table_column_names = sql_cursor.fetchall()

    column_names = list()

    for name in table_column_names:
        column_names.append(name[1])

    return column_names



def main():
    database = r"sales_reporting_sqlite.db"

    sql_create_customers_table = """ CREATE TABLE IF NOT EXISTS dim_customers (
                                        id integer,
                                        name text NOT NULL,
                                        email text,
                                    FOREIGN KEY (id) REFERENCES sales_fact (customer_id)

                                    ); """


    sql_create_orders_table = """CREATE TABLE IF NOT EXISTS dim_orders (
                                    id integer,
                                    customer_id integer NOT NULL,
                                    total_price_usd numeric(10,2),
                                    total_price_cad numeric(10,2),
                                    order_date text NOT NULL,
                                    FOREIGN KEY (id) REFERENCES sales_fact (order_id)
                                );"""
    
    sql_create_order_lines_table = """CREATE TABLE IF NOT EXISTS dim_order_lines (
                                id integer,
                                order_id integer NOT NULL,
                                product_id integer NOT NULL,
                                value_usd numeric(10,2),
                                value_cad numeric(10,2),
                                FOREIGN KEY (id) REFERENCES sales_fact (line_id)
                            );"""

    sql_create_products_table = """CREATE TABLE IF NOT EXISTS dim_products (
                                id integer,
                                product_sku text NOT NULL,
                                product_name text NOT NULL,
                                FOREIGN KEY (id) REFERENCES sale_fact (product_id)
                            );"""

    sql_create_sales_fact_table = """CREATE TABLE IF NOT EXISTS sales_fact (
                                order_id integer,
                                customer_id integer,
                                line_id integer,
                                product_id integer,
                                value_usd numeric(10,2),
                                value_cad numeric(10,2),
                                FOREIGN KEY (order_id) REFERENCES orders (id),
                                FOREIGN KEY (order_id) REFERENCES orders (id),
                                FOREIGN KEY (order_id) REFERENCES orders (id),
                                FOREIGN KEY (order_id) REFERENCES orders (id)
                            );"""
    
    #get json data
    f = open('orders.json')
    q1_orders = json.load(f)
    f.close()

    #get api exchange rates
    response = requests.get('https://api.exchangeratesapi.io/history?start_at=2020-01-01&end_at=2020-03-31&base=USD&symbols=CAD')
    response.raise_for_status()
    fx = json.loads(response.text)

    #associate exchange rates with orders
    df_q1 = pd.json_normalize(q1_orders)
    df_q1['order_date'] = df_q1['created_at'].apply(lambda x: x[:10])

    df_fx = pd.DataFrame(fx)
    df_fx['cad_rates'] = df_fx['rates'].apply(lambda x: x['CAD'])
    df_fx_cad = df_fx['cad_rates']

    
    df_join_q1_fx = pd.merge(left=df_q1,right=df_fx_cad,left_on='order_date',right_index=True,how='left')
    df_join_q1_fx['cad_rates'].fillna(df_join_q1_fx['cad_rates'].mean(),inplace =True)
    df_join_q1_fx['total_price_cad'] = round((df_join_q1_fx['total_price'] * df_join_q1_fx['cad_rates']),2)
    df_join_q1_fx.reset_index(inplace=True)

    #dim tables
    df_orders = df_join_q1_fx[['id','customer.id','total_price','total_price_cad','order_date']]

    df_order_lines_s1 = json_normalize(q1_orders, record_path='line_items',
                                meta = ['id','product_id','product_sku','product_name'],
                                errors='ignore',record_prefix='line_')
    df_order_lines_s2 = pd.merge(left=df_order_lines_s1,right= df_join_q1_fx, left_on='id',right_on='id')
    df_order_lines_s2['line_price_cad'] = round((df_order_lines_s2['line_price'] * df_order_lines_s2['cad_rates']),2)
    df_order_lines_final = df_order_lines_s2[['line_id','id','line_product_id','line_price','line_price_cad']]

    df_products = df_order_lines_s2[['line_product_id','line_product_sku','line_product_name']].drop_duplicates()

    df_customers = df_q1[['customer.id','customer.name','customer.email']].drop_duplicates()

    #fact table
    df_order_cust = df_join_q1_fx[['id','customer.id']]
    df_order_line_prod = df_order_lines_final[['id','line_id','line_product_id','line_price','line_price_cad']]
    df_sales_fact = pd.merge(left=df_order_cust,right=df_order_line_prod,on='id')
    

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        create_table(conn, sql_create_customers_table)
        create_table(conn, sql_create_orders_table)
        create_table(conn, sql_create_order_lines_table)
        create_table(conn, sql_create_products_table)
        create_table(conn, sql_create_sales_fact_table)
    else:
        print("Error! cannot create the database connection.")

    #insert to tables
    insert_values_to_table(conn, 'dim_orders', df_orders)
    insert_values_to_table(conn, 'dim_customers', df_customers)
    insert_values_to_table(conn, 'dim_order_lines', df_order_lines_final)
    insert_values_to_table(conn, 'dim_products', df_products)
    insert_values_to_table(conn, 'sales_fact', df_sales_fact)


    df_orders.to_csv('./output/orders.csv',index=False)
    df_customers.to_csv('./output/customers.csv',index=False)
    df_order_lines_final.to_csv('./output/order_lines.csv',index=False)
    df_products.to_csv('./output/products.csv',index=False)
    df_sales_fact.to_csv('./output/sales_fact.csv',index=False)

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()