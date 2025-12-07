import pandas as pd
from pathlib import Path
from pandas.tseries.holiday import USFederalHolidayCalendar

# ============================================================
#                     CONFIGURATION
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[0]

CLEAN_DIR = Path("/clean_data")

OUT_DIR = CLEAN_DIR / "dimensions"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Change these if needed
START_DATE = "1950-01-01"
END_DATE   = "2050-12-31"

# ============================================================
#                BUILD DIM DATE DATAFRAME
# ============================================================

def build_dim_date(start=START_DATE, end=END_DATE):
    print(f"[INFO] Building date dimension from {start} to {end}...")

    # Create daily date range
    dates = pd.date_range(start=start, end=end, freq="D")

    df = pd.DataFrame()
    df["date_full"] = dates
    df["date_key"] = dates.strftime("%Y%m%d").astype("int64")

    # Day attributes
    df["date_day"] = dates.day
    df["date_day_name"] = dates.strftime("%A")

    # Month attributes
    df["date_month"] = dates.month
    df["date_month_name"] = dates.strftime("%B")

    # Quarter attributes
    df["date_quarter"] = dates.quarter
    df["date_quarter_name"] = df["date_quarter"].apply(lambda q: f"Q{q}")

    # Half-year attributes
    df["date_half_year"] = ((dates.month - 1) // 6) + 1
    df["date_half_year_name"] = df["date_half_year"].apply(lambda h: f"H{h}")

    # Year
    df["date_year"] = dates.year

    # Weekend?
    df["date_is_weekend"] = dates.weekday >= 5

    # U.S. Holidays example (can remove or swap for PH holidays later)
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=start, end=end)
    df["date_is_holiday"] = df["date_full"].isin(holidays)

    print("[INFO] dimDate shape:", df.shape)

    return df


# ============================================================
#                       MAIN EXECUTION
# ============================================================

def main():
    print("\n========== BUILDING dimDate ==========\n")
    
    dim = build_dim_date()

    csv_path = OUT_DIR / "dimDate.csv"
    parquet_path = OUT_DIR / "dimDate.parquet"

    dim.to_csv(csv_path, index=False)
    dim.to_parquet(parquet_path, index=False)

    print(f"[OK] Saved dimDate CSV → {csv_path}")
    print(f"[OK] Saved dimDate Parquet → {parquet_path}")
    print(f"[DONE] dimDate generation complete.\n")


if __name__ == "__main__":
    main()
