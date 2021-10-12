
import requests
import pandas as pd


## Waiting until thingss break
per_page=250
page = 1
json_df = []

while True:
    print("----")
    url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page={per_page}&page={page}"
    print("Requesting", url)
    ## API call
    response = requests.get(url)
    data = response.json()
    
    # Debug by printing out the first data extracted
    try:
        print(data[0])
    except:
        print("There was an error!")
        break

    ## if we did find crypto asset, add them 
    ## to our list and then move on to the next
    json_df.extend(data)
    page = page + 1   

## Convert to a dataframe and subject the necessary columns
df_coin = pd.DataFrame(json_df, columns=['id', 'name', 'current_price', 'total_volume', 'market_cap', 'last_updated'])
# Output file
   # write out
upload_dir = 'C:/Users/Dee/root/Projects/personal_real_projects/becoming_data_engineer/coindecko/'

df_coin.to_csv(upload_dir + "data/df_coin_data.csv", index=False)
