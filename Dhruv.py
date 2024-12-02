#!/usr/bin/env python
# coding: utf-8

# In[2]:


from cassandra.cluster import Cluster
import pandas as pd

cluster = Cluster(['localhost'])
session = cluster.connect()

session.set_keyspace('sales_data')

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sales_data
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

url = 'https://raw.githubusercontent.com/gchandra10/filestorage/main/sales_100.csv'
df = pd.read_csv(url)

df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce').dt.date
df['Ship Date'] = pd.to_datetime(df['Ship Date'], errors='coerce').dt.date

def create_table(query):
    session.execute(query)

def prepare_and_insert_data(df, table_name, insert_statement):
    data = [
        (
            row['Order ID'], row['Region'], row['Country'], row['Item Type'], row['Sales Channel'], 
            row['Order Priority'], row['Order Date'], row['Ship Date'], row['UnitsSold'], 
            row['UnitPrice'], row['UnitCost'], row['TotalRevenue'], row['TotalCost'], row['TotalProfit']
        )
        for _, row in df.iterrows()
    ]
    for entry in data:
        session.execute(insert_statement, entry)

create_table("""
    CREATE TABLE IF NOT EXISTS bronze_sales (
        order_id INT PRIMARY KEY,
        region TEXT,
        country TEXT,
        item_type TEXT,
        sales_channel TEXT,
        order_priority TEXT,
        order_date DATE,
        ship_date DATE,
        units_sold INT,
        unit_price FLOAT,
        unit_cost FLOAT,
        total_revenue FLOAT,
        total_cost FLOAT,
        total_profit FLOAT
    )
""")

insert_bronze = session.prepare("""
    INSERT INTO bronze_sales (order_id, region, country, item_type, sales_channel, order_priority, 
                              order_date, ship_date, units_sold, unit_price, unit_cost, total_revenue, 
                              total_cost, total_profit) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

prepare_and_insert_data(df, 'bronze_sales', insert_bronze)

create_table("""
    CREATE TABLE IF NOT EXISTS silver_sales (
        order_id INT PRIMARY KEY,
        region TEXT,
        country TEXT,
        item_type TEXT,
        sales_channel TEXT,
        order_priority TEXT,
        order_date DATE,
        ship_date DATE,
        units_sold INT,
        unit_price FLOAT,
        unit_cost FLOAT,
        total_revenue FLOAT,
        total_cost FLOAT,
        total_profit FLOAT
    )
""")

insert_silver = session.prepare("""
    INSERT INTO silver_sales (order_id, region, country, item_type, sales_channel, order_priority, 
                              order_date, ship_date, units_sold, unit_price, unit_cost, total_revenue, 
                              total_cost, total_profit) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

prepare_and_insert_data(df, 'silver_sales', insert_silver)

create_table("""
    CREATE TABLE IF NOT EXISTS gold_sales_region (
        region TEXT PRIMARY KEY,
        total_sales FLOAT
    )
""")

region_sales = df.groupby('Region')['TotalRevenue'].sum().reset_index()

for _, row in region_sales.iterrows():
    session.execute("""
        INSERT INTO gold_sales_region (region, total_sales)
        VALUES (%s, %s)
    """, (row['Region'], row['TotalRevenue']))

create_table("""
    CREATE TABLE IF NOT EXISTS gold_sales_country (
        country TEXT PRIMARY KEY,
        total_sales FLOAT
    )
""")

country_sales = df.groupby('Country')['TotalRevenue'].sum().reset_index()

for _, row in country_sales.iterrows():
    session.execute("""
        INSERT INTO gold_sales_country (country, total_sales)
        VALUES (%s, %s)
    """, (row['Country'], row['TotalRevenue']))

create_table("""
    CREATE TABLE IF NOT EXISTS gold_sales_item_type (
        item_type TEXT PRIMARY KEY,
        total_sales FLOAT
    )
""")

item_sales = df.groupby('Item Type')['TotalRevenue'].sum().reset_index()

for _, row in item_sales.iterrows():
    session.execute("""
        INSERT INTO gold_sales_item_type (item_type, total_sales)
        VALUES (%s, %s)
    """, (row['Item Type'], row['TotalRevenue']))

gold_region_data = session.execute("SELECT * FROM gold_sales_region")
for row in gold_region_data:
    print(row)

gold_country_data = session.execute("SELECT * FROM gold_sales_country")
for row in gold_country_data:
    print(row)

gold_item_type_data = session.execute("SELECT * FROM gold_sales_item_type")
for row in gold_item_type_data:
    print(row)


# In[ ]:




