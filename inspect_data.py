import pandas as pd

df = pd.read_csv("agrorisk_main.csv")
print("Shape:", df.shape)
print("\nMissing values per column:\n", df.isnull().sum()[df.isnull().sum() > 0])
print("\nDuplicate record_ids:", df['record_id'].duplicated().sum())
print("\nData Types:\n", df.dtypes)