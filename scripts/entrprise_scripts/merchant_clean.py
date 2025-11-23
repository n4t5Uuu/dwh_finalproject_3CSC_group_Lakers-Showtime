# clean_merchant_names_and_contact.py

import pandas as pd
from pathlib import Path
import re

# ----- CONFIG ----- #
SCRIPT_DIR = Path(__file__).resolve().parent 

INPUT_FILE = SCRIPT_DIR / "merchant_data.csv"
OUTPUT_FILE = SCRIPT_DIR / "merchant_data_clean.csv"
# ------------------ #


def normalize_phone(phone: str) -> str:
    """Same rule as staff: make formatting consistent."""
    if pd.isna(phone):
        return ""

    digits = re.sub(r"\D", "", str(phone))

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"

    return digits


def clean_name(name: str) -> str:
    """
    Remove inconsistent outer quotation marks and extra spaces.

    E.g.  '"Ontodia, Inc"' -> 'Ontodia, Inc'
          '  "Whitby Group"  ' -> 'Whitby Group'
    """
    if pd.isna(name):
        return ""

    cleaned = str(name).strip()
    cleaned = cleaned.strip('"').strip("'")  # remove outer quotes
    cleaned = re.sub(r"\s+", " ", cleaned)   # collapse multiple spaces

    return cleaned


def main():
    print(f"Reading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # Clean names
    df["name"] = df["name"].astype(str).apply(clean_name)

    # Normalize contact numbers
    df["contact_number"] = df["contact_number"].astype(str)
    df["contact_number"] = df["contact_number"].apply(normalize_phone)

    print("Sample after cleaning:")
    print(df[["merchant_id", "name", "contact_number"]].head())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved cleaned merchant_data (names + phones) to:\n  {OUTPUT_FILE}")


if __name__ == "__main__":
    main()