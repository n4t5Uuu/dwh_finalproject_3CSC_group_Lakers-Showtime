# campaign_clean.py

import pandas as pd
from pathlib import Path
import re

# ----- CONFIG: use same folder as this script ----- #
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_FILE = SCRIPT_DIR / "campaign_data.csv"
OUTPUT_FILE = SCRIPT_DIR / "campaign_data_clean.csv"
# -------------------------------------------------- #


def normalize_discount(val: str) -> str:
    """
    Make discount values consistent.
    Examples:
      '1%' -> '1%'
      '1pct' -> '1%'
      '1 percent' -> '1%'
      '10PERCENT' -> '10%'
    """
    if pd.isna(val):
        return ""

    text = str(val).strip().lower()

    # Find the first number in the string (integer or decimal)
    match = re.search(r"(\d+(\.\d+)?)", text)
    if not match:
        # If no number, just return original cleaned text
        return text

    num = float(match.group(1))

    # If it's essentially an integer, drop the .0
    if num.is_integer():
        num_str = str(int(num))
    else:
        num_str = str(num)

    return f"{num_str}%"


def clean_description(desc: str) -> str:
    """
    Remove unnecessary outer quotation marks and extra spaces.

    e.g.
      '"Summer Promo for new users"' -> 'Summer Promo for new users'
      "  'Back-to-school discount'  " -> 'Back-to-school discount'
    """
    if pd.isna(desc):
        return ""

    cleaned = str(desc).strip()
    # remove leading/trailing quotes (both " and ')
    cleaned = cleaned.strip('"').strip("'")
    # collapse multiple spaces to a single space
    cleaned = re.sub(r"\s+", " ", cleaned)

    return cleaned


def main():
    print(f"Reading campaign data from: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # Clean discount column
    if "discount" in df.columns:
        df["discount"] = df["discount"].apply(normalize_discount)
    else:
        print("Warning: 'discount' column not found in campaign_data.csv")

    # Clean campaign_description column
    if "campaign_description" in df.columns:
        df["campaign_description"] = df["campaign_description"].apply(
            clean_description
        )
    else:
        print("Warning: 'campaign_description' column not found in campaign_data.csv")

    print("\nSample after cleaning:")
    print(df.head())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved cleaned campaign data to:\n  {OUTPUT_FILE}")


if __name__ == "__main__":
    main()