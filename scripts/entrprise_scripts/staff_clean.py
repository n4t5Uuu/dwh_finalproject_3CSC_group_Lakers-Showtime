# clean_staff_contact_numbers.py

import pandas as pd
from pathlib import Path
import re

# ----- CONFIG ----- #
SCRIPT_DIR = Path(__file__).resolve().parent 

INPUT_FILE = SCRIPT_DIR / "staff_data.csv"
OUTPUT_FILE = SCRIPT_DIR / "staff_data_clean.csv"
# ------------------ #


def normalize_phone(phone: str) -> str:
    """
    Normalize phone numbers to a consistent format.

    - Keep only digits.
    - If 10 digits -> XXX-XXX-XXXX
    - If 11 digits starting with '1' -> drop leading 1, format as above
    - Otherwise -> return digits string as-is (at least consistent).
    """
    if pd.isna(phone):
        return ""

    digits = re.sub(r"\D", "", str(phone))

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"

    # Fallback: just return the digits so it’s still consistent
    return digits


def main():
    print(f"Reading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # Make sure it's string before normalization
    df["contact_number"] = df["contact_number"].astype(str)

    # Apply normalization
    df["contact_number"] = df["contact_number"].apply(normalize_phone)

    print("Sample after phone normalization:")
    print(df[["staff_id", "name", "contact_number"]].head())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved cleaned staff_data with normalized phone numbers to:\n  {OUTPUT_FILE}")


if __name__ == "__main__":
    main()