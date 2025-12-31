import pandas as pd
import os

storage_options = {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {
        "endpoint_url": "http://localhost:9010"
    }
}

file_path = "s3://datalake/silver/user_profile/"

try:
    df = pd.read_parquet(file_path, storage_options=storage_options)
    
    print(f"--- Đọc dữ liệu từ: {file_path} ---")
    print(f"Tổng số dòng: {len(df)}")
    print("\n--- 10 dòng đầu tiên ---")
    print(df.head(10).to_string())
    
    print("\n--- Schema (Kiểu dữ liệu) ---")
    print(df.dtypes)

except Exception as e:
    print(f"Lỗi: {e}")