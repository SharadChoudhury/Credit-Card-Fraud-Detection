import pandas as pd

df = pd.read_csv("card_transactions.csv", header=0)

print(df.head())
print(df.info())