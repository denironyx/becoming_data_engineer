from getpass import getpass
from dns.resolver import query
from mysql.connector import connect, Error
import pandas as pd

df = pd.read_csv('df_coin4.csv', index_col=False, delimiter=',')

print(df.head())


create_crypto_asset_table_query = """
CREATE table crypto_asset(
    id INT(64) NOT NULL AUTO_INCREMENT,
    crypto_id VARCHAR(255),
    crypto_name VARCHAR(255),
    current_price NUMERIC(15, 2),
    total_volume_24h NUMERIC(30, 2),
    market_cap NUMERIC(30, 2),
    last_update VARCHAR(255),
    PRIMARY KEY (id)
);
"""


try:
    with connect(
        host="localhost",
        user=input("Enter username: "),
        password=getpass("Enter password: "),
        database = "coindecko_db"
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
            ## loop through the data frame
            for i,row in df.iterrows():
                insert_query = "INSERT INTO coindecko_db.crypto_asset(crypto_id, crypto_name, current_price, total_volume_24h, market_cap, last_update) VALUES (%s,%s,%s,%s,%s,%s)"
                cursor.execute(insert_query, tuple(row)) 
                print("Record inserted")
                connection.commit() 
except Error as e:
    print("Error while connecting to Mysql", e)

