
import pandas as pd

upload_dir = 'data/'

df_coin = pd.read_csv(upload_dir + 'df_coin_data.csv')


spec_chars = ["!",'"',"#","%","&","'","(",")",
              "*","+",",","-",".","/",":",";","<",
              "=",">","?","@","[","\\","]","^","_",
              "`","{","|","}","~","–"]

for char in spec_chars:
    df_coin['name'] = df_coin['name'].str.replace(char, '')


## Duplicate Checker
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
                    

dfa = duplicate_checker(df_coin)   


print(dfa)

print(dfa.isna().sum())

dfa['current_price'] = dfa['current_price'].fillna(0)
dfa['market_cap'] = dfa['market_cap'].fillna(0)
dfa['total_volume'] = dfa['total_volume'].fillna(0)
dfa['last_updated'].fillna('Null', inplace=True)

dfa.to_csv(upload_dir + 'data/df.csv', index=False)
print(dfa.head())


