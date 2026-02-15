import pandas as pd
import re

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:

    def clean_name(name: str) -> str:
        s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
        s2 = re.sub(r'[^a-zA-Z0-9]+', '_', s1)
        return s2.lower().strip('_')

    df.columns = [clean_name(col) for col in df.columns]
    return df