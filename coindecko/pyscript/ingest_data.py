import requests
import pandas as pd

# Waiting until things break
per_page = 250
page = 1
json_df = []

# Write a function to test if the first id is bitcoin, if it's true the convert the data to a dataframe


def test_data_ingested(df):
    if df[0]['id'] == 'bitcoin':
        df = pd.DataFrame(df, columns=['id', 'name', 'current_price', 'total_volume', 'market_cap', 'last_updated'])
        return df
    else:
        print('Wrong data imported! Kindly check the data source')


while True:
    print("----")
    url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page={per_page}&page={page}"
    print("Requesting", url)
    # Do the HTTP get request
    response = requests.get(url)
    # Code here will only run if the request is successful
    if response.status_code == 200:
        print('The request was a success!')
    # Code here will react to failed requests
    else:
        print('There is a failed request! ', response.status_code)
        # Decode the JSON response into a dictionary and use the data
    data = response.json()
    # Debug by printing out the first data extracted
    print(data[0])
    # if we did find crypto asset, add them
    # to our list and then move on to the next
    json_df.extend(data)
    page = page + 1

# Convert to a dataframe and subject the necessary columns
df_coin = test_data_ingested(json_df)
# Output file
# write out
upload_dir = 'data/'

df_coin.to_csv(upload_dir + "df_coin_data.csv", index=False)
