import pandas as pd
import re
from pathlib import Path
import os

if os.path.exists("/data_files/Marketing Department"):
    RAW_DIR = Path("/data_files/Marketing Department")
else:
    RAW_DIR = Path("./data_files/Marketing Department")

CLEANED_DIR = Path("Cleaned Folder/Marketing Department")
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

def clean_campaign_data():
    fname = "campaign_data.csv"
    src_path = RAW_DIR / fname
    if not src_path.exists():
        print(f"File not found: {src_path}")
        return
    # Read and split lines by tabs
    lines = open(src_path, encoding='utf-8').readlines()
    rows = [line.strip('\n').split('\t') for line in lines if line.strip()]
    rows = [[cell.strip() for cell in row] for row in rows]
    header, data = rows[0], rows[1:]
    df = pd.DataFrame(data, columns=header)
    print("COLUMNS:", df.columns)
    print(df.head())

    # Remove ALL quotes everywhere in every string column (aggressive clean)
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.replace(r'"', '', regex=True).str.replace(r"'", '', regex=True).str.strip()

    # Clean 'discount'
    def clean_discount(val):
        text = str(val).lower().replace('percent', '%').replace('pct', '%')
        match = re.search(r'(\d+(?:\.\d+)?)', text)
        if match:
            num = match.group(1)
            return f"{int(float(num))}%" if float(num).is_integer() else f"{num}%"
        return text.strip()

    if "discount" in df.columns:
        df["discount"] = df["discount"].apply(clean_discount)

    # Clean 'campaign_description' (removes any missed edge quotes, again)
    if "campaign_description" in df.columns:
        df["campaign_description"] = (
            df["campaign_description"]
            .astype(str)
            .str.replace(r'^"+|"+$', '', regex=True)
            .str.replace(r"^'+|'+$", '', regex=True)
            .str.strip()
        )

    cleaned_path = CLEANED_DIR / fname
    df.to_csv(cleaned_path, index=False)
    print(f"[campaign_data] Saved cleaned file to {cleaned_path}")

def clean_transactional_campaign_data():
    fname = "transactional_campaign_data.csv"
    src_path = RAW_DIR / fname
    if not src_path.exists():
        print(f"File not found: {src_path}")
        return
    df = pd.read_csv(src_path)
    print("COLUMNS:", df.columns)
    print(df.head())

    # Clean and rename 'estimated arrival'
    if "estimated arrival" in df.columns:
        df["estimated_arrival_in_days"] = (
            df["estimated arrival"].astype(str)
                .str.extract(r"(\d+)")
                .astype(float)
                .astype("Int64")
        )
        df.drop(columns=["estimated arrival"], inplace=True)

    cleaned_path = CLEANED_DIR / fname
    df.to_csv(cleaned_path, index=False)
    print(f"[transactional_campaign_data] Saved cleaned file to {cleaned_path}")

def main():
    clean_campaign_data()
    clean_transactional_campaign_data()

if __name__ == "__main__":
    main()
