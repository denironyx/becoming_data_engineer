
import pandas as pd

upload_dir = 'data/'

df_coin = pd.read_csv(upload_dir + 'df_coin_data.csv')

## Perform some data cleaning with this 
spec_chars = ["!",'"',"#","%","&","'","(",")",
              "*","+",",","-",".","/",":",";","<",
              "=",">","?","@","[","\\","]","^","_",
              "`","{","|","}","~","–"]

## remove special characters from the dataframe
for char in spec_chars:
    df_coin['name'] = df_coin['name'].str.replace(char, '')

# Duplicate Checker
# If the function 
def duplicate_checker(values):
        def has_duplicates(values):
            if len(values) != len(set(values)):
                return True
            else:
                return False
        if has_duplicates(values):
            duplicate_in_coin = values.duplicated(subset=['id'])
            if duplicate_in_coin.any():
                values = values.loc[~duplicate_in_coin].reset_index(drop = True)
                return values
                    
## applying the dupplicate checker function
dfa = duplicate_checker(df_coin)   

# working with missing values
dfa['current_price'] = dfa['current_price'].fillna(0)
dfa['market_cap'] = dfa['market_cap'].fillna(0)
dfa['total_volume'] = dfa['total_volume'].fillna(0)
dfa['last_updated'].fillna('Null', inplace=True)

## Exporting the process data
dfa.to_csv(upload_dir + 'df.csv', index=False)
print(dfa.head())


