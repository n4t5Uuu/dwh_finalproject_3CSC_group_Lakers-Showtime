# user_job_clean.py

import pandas as pd
from pathlib import Path

# ---------- CONFIG ---------- #
SCRIPT_DIR = Path(__file__).resolve().parent 

INPUT_FILE = SCRIPT_DIR / "user_job.csv"
OUTPUT_FILE = SCRIPT_DIR / "user_job_clean.csv"
# ---------------------------- #


def main():
    print(f"Reading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # Drop auto-generated index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize strings a bit just in case
    df["job_title"] = df["job_title"].astype(str).str.strip()

    # Mask: rows where job_title == "Student" and job_level is null
    is_student = df["job_title"].str.lower() == "student"
    is_level_null = df["job_level"].isna()

    fix_mask = is_student & is_level_null
    num_to_fix = fix_mask.sum()

    # Set job_level = "Student" for those rows
    df.loc[fix_mask, "job_level"] = "Student"

    # Save cleaned file
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Rows fixed (Student with null job_level): {num_to_fix}")
    print(f"Saved cleaned user_job to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
