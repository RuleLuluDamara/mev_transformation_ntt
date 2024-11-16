import os
import pandas as pd
from constant import DATA_PATH

def load_data_local():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"File '{DATA_PATH}' tidak ditemukan.")
    
    df = pd.read_excel(DATA_PATH)
    
    required_columns = {'ME_CODE', 'ME_PERIOD', 'ME_VAL'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Dataset harus mengandung kolom {required_columns}.")
    
    return df
