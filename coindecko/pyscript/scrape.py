import pandas as pd

df = pd.read_json('/data/coindecko.json')

print(df.head())