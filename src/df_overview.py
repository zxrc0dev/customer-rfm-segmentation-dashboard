import pandas as pd

def df_overview(df):
    print(f"{'='*33} Shape {'='*33}")
    print(df.shape)
    print(f"{'='*33} Info {'='*33}")
    print(df.info())
    print(f"{'='*33} Columns {'='*33}")
    print(df.columns)
    print(f"{'='*33} Describe {'='*33}")
    print(df.describe())
    print(f"{'='*33} NaN {'='*33}")
    print(df.isnull().sum())
    print(f"{'='*33} Duplicates {'='*33}")
    print(df.duplicated().sum())
    print(f"{'='*33} Cardinality & Top Values {'='*33}")
    for c in df.select_dtypes(include='object').columns:
        print(c, df[c].nunique(), df[c].value_counts(normalize=True).head())