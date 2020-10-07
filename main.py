import numpy as np
import pandas as pd
import json, requests
import sqlite3
from sqlite3 import Error
from pandas.io.json import json_normalize

#get json data
f = open('orders.json')
q1_orders = json.load(f)
f.close()

#get api exchange rates
response = requests.get('https://api.exchangeratesapi.io/history?start_at=2020-01-01&end_at=2020-03-31&base=USD&symbols=CAD')
response.raise_for_status()
fx = json.loads(response.text)

#create two dataframes joined on orderdate
df_q1 = pd.json_normalize(q1_orders)
df_fx = pd.DataFrame(fx)
df_fx['cad_rates'] = df_fx['rates'].apply(lambda x: x['CAD'])
df_q1['order_date'] = df_q1['created_at'].apply(lambda x: x[:10])
df_join_q1_fx = pd.merge(left=df_q1,right=df_fx,left_on='order_date',right_index=True)
df_join_q1_fx['total_price_cad'] = round((df_join_q1_fx['total_price'] * df_join_q1_fx['cad_rates']),2)
df_join_q1_fx.reset_index(inplace=True)

#create the dataframes corresponding the sql tables orders, orderlines, products, and customers

df_orders = df_join_q1_fx[['id','customer.id','total_price','total_price_cad']]

df_order_lines_s1 = json_normalize(q1_orders, record_path='line_items',
                               meta = ['id','product_id','product_sku','product_name'],
                               errors='ignore',record_prefix='line_')
df_order_lines_s2 = pd.merge(left=df_order_lines_s1,right= df_join_q1_fx, left_on='id',right_on='id')
df_order_lines_s2['line_price_cad'] = round((df_order_lines['line_price'] * df_order_lines['cad_rates']),2)
df_order_lines_final = df_order_lines_s2[['line_id','id','line_product_id','line_price','line_price_cad']]

df_products = df_order_lines[['line_product_id','line_product_sku','line_product_name']].drop_duplicates()

df_customers = df_join_q1_fx[['customer.id','customer.name','customer.email']].drop_duplicates()



#task two creating the sql tables

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def main():
    database = r"/sales_reporting_sqlite.db"

    sql_create_customers_table = """ CREATE TABLE IF NOT EXISTS customers (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        email text
                                    ); """

    sql_create_orders_table = """CREATE TABLE IF NOT EXISTS orders (
                                    id integer PRIMARY KEY,
                                    customer_id integer NOT NULL,
                                    total_price_usd numeric(10,2),
                                    total_price_cad numeric(10,2),
                                    order_date text NOT NULL,
                                    FOREIGN KEY (customer_id) REFERENCES customers (id)
                                );"""
    
    sql_create_order_lines_table = """CREATE TABLE IF NOT EXISTS order_lines (
                                id integer PRIMARY KEY,
                                order_id integer NOT NULL,
                                product_id integer NOT NULL,
                                price_usd numeric(10,2),
                                price_cad numeric(10,2),
                                FOREIGN KEY (order_id) REFERENCES orders (id),
                                FOREIGN KEY (product_id) REFERENCES products (id)
                            );"""

    sql_create_products_table = """CREATE TABLE IF NOT EXISTS products (
                                id integer PRIMARY KEY,
                                product_name text NOT NULL,
                                product_sku text NOT NULL
                            );"""


    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        create_table(conn, sql_create_customers_table)
        create_table(conn, sql_create_orders_table)
        create_table(conn, sql_create_order_lines_table)
        create_table(conn, sql_create_products_table)
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()