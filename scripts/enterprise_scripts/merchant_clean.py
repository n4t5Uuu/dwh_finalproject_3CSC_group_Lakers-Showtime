import pandas as pd
from pathlib import Path
import re

# ----- CONFIG ----- #

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_FILE = PROJECT_ROOT / "data_files" / \
    "Enterprise Department" / "merchant_data.csv"
OUTPUT_FILE = PROJECT_ROOT / "clean_data" / \
    "enterprise" / "merchant_data_clean.csv"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ------------------ #


def normalize_phone(phone: str) -> str:
    """Normalize phone number similar to staff."""
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
    E.g. '"Ontodia, Inc"' -> 'Ontodia, Inc'
         ' "Whitby Group" ' -> 'Whitby Group'
    """
    if pd.isna(name):
        return ""

    cleaned = str(name).strip()
    cleaned = cleaned.strip('"').strip("'")       # remove outer quotes
    cleaned = re.sub(r"\s+", " ", cleaned)        # collapse multiple spaces
    return cleaned


def main():
    print(f"Reading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # Force all merchant country values to United States
    df["country"] = "United States"

    # Clean names
    df["name"] = df["name"].astype(str).apply(clean_name)

    # Normalize contact_number
    df["contact_number"] = df["contact_number"].astype(str)
    df["contact_number"] = df["contact_number"].apply(normalize_phone)

    print("Sample after cleaning:")
    print(df[["merchant_id", "name", "country", "contact_number"]].head())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved cleaned merchant_data to:\n {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
