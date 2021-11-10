from mysql.connector import connect, Error
import pandas as pd
import os

MyPASS = os.environ.get('MyPASS')  # set your password
print(MyPASS)
upload_dir = 'data/'

df = pd.read_csv(upload_dir + 'df.csv', index_col=False, delimiter=',')

print(df.head())

# Create the first connection
try:
    with connect(
            host='localhost',
            user='root',
            password=MyPASS  # change the password here
    ) as connection:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("DROP DATABASE IF EXISTS coingecko_db;")
            cursor.execute("CREATE DATABASE coingecko_db;")
            print("Database is created")
except Error as e:
    print("Error while connecting to MySQL", e)

# Create a table
create_crypto_asset_table_query = """
CREATE table crypto_asset(
    id INT(64) NOT NULL AUTO_INCREMENT,
    crypto_id VARCHAR(255),
    crypto_name VARCHAR(255),
    current_price NUMERIC(30,2),
    total_volume_24h NUMERIC(30,2),
    market_cap NUMERIC(30, 2),
    last_update VARCHAR(255),
    created_on DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);
"""

# Create another connection
try:
    with connect(
            host='localhost',
            user='root',
            password=MyPASS,
            database="coingecko_db"
    ) as connection:
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            cursor.execute("DROP TABLE IF EXISTS crypto_asset;")
            print('Creating table....')
            cursor.execute(create_crypto_asset_table_query)
            print("Table is created...")
            # loop through the data frame
            for i, row in df.iterrows():
                insert_query = "INSERT INTO coingecko_db.crypto_asset(crypto_id, crypto_name, current_price, " \
                               "total_volume_24h, market_cap, last_update) VALUES (%s,%s,%s,%s,%s,%s) "
                cursor.execute(insert_query, tuple(row))
                print("Record inserted")
                connection.commit()
except Error as e:
    print("Error while connecting to Mysql", e)
