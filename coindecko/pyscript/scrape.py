import pandas as pd
import requests

url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=50&page=1"
response = requests.get(url)
response

if (response.status_code == 200):
    print('The request was a success!')
        # Code here will react to failed requests
else:
    print('There is a failed request! ', response.status_code)
        
    # Decode the JSON response into a dictionary and use the data
data = response.json()

## Write a function to test if the first id is bitcoin, if it's true the convert the data to a dataframe
def test_data_ingested(json_df):
    if (json_df[0]['id'] == 'bitcoin'):
        df = pd.DataFrame(json_df, columns=['id', 'name', 'current_price', 'total_volume', 'market_cap', 'last_updated'])
        return df
    else:
        print('Wrong data imported! Kindly check the data source')

dfa = test_data_ingested(data)