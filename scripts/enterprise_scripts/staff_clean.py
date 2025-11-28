import pandas as pd
from pathlib import Path
import re

# ----- CONFIG ----- #

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_FILE = PROJECT_ROOT / "data_files" / "Enterprise Department" / "staff_data.csv"
OUTPUT_FILE = PROJECT_ROOT / "clean_data" / "enterprise" / "staff_data_clean.csv"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ------------------ #

def normalize_phone(phone: str) -> str:
    """
    Normalize phone numbers to a consistent format.

    - Keep only digits.
    - If 10 digits -> XXX-XXX-XXXX
    - If 11 digits starting with '1' -> drop leading 1, format as above
    - Otherwise -> return digits string as-is.
    """
    if pd.isna(phone):
        return ""

    digits = re.sub(r"\D", "", str(phone))

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"

    return digits

def main():
    print(f"Reading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # Force all staff country values to United States
    df["country"] = "United States"

    # Normalize contact_number
    df["contact_number"] = df["contact_number"].astype(str)
    df["contact_number"] = df["contact_number"].apply(normalize_phone)

    print("Sample after cleaning:")
    print(df[["staff_id", "name", "country", "contact_number"]].head())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved cleaned staff_data to:\n {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
