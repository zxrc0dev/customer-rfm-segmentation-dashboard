import pandas as pd

def df_info(df):
    print(f"{'='*30} Head {'='*30}")
    print(df.head())
    print(f"{'='*30} Tail {'='*30}")
    print(df.tail())
    print(f"{'='*30} Shape {'='*30}")
    print(df.shape)
    print(f"{'='*30} Info {'='*30}")
    print(df.info())
    print(f"{'='*30} Columns {'='*30}")
    print(df.columns)
    print(f"{'='*30} Describe {'='*30}")
    print(df.describe())
    print(f"{'='*30} NaN {'='*30}")
    print(df.isnull().sum())
    print(f"{'='*30} Duplicates {'='*30}")
    print(df.duplicated().sum())
    print(f"{'='*30} Cardinality & Top Values {'='*30}")
    for c in df.select_dtypes(include='object').columns:
        print(c, df[c].nunique(), df[c].value_counts(normalize=True).head())