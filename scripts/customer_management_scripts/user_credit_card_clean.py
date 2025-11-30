import pandas as pd
from pathlib import Path

# ---------- CONFIG ---------- #
# .../scripts/customer_management_scripts
SCRIPT_DIR = Path(__file__).resolve().parent
# .../dwh_finalproject_3CSC_group_Lakers-Showtime
PROJECT_ROOT = SCRIPT_DIR.parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data_files" / "Customer Management Department"
CLEAN_DATA_DIR = PROJECT_ROOT / "clean_data" / "customer_management"

# input CSVs
INPUT_FILE = RAW_DATA_DIR / "user_credit_card.csv"
USER_DATA_FILE = RAW_DATA_DIR / "user_data.csv"

# output cleaned CSV
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = CLEAN_DATA_DIR / "user_credit_card.csv"
# ---------------------------- #


def _digits_from_user_id(uid: str) -> str:
    """Extract numeric part from something like 'USER012195'."""
    return "".join(ch for ch in uid if ch.isdigit())


def build_user_key_mapping_from_user_data(user_data_path: Path) -> pd.DataFrame:
    """
    Read user_data.csv and create a mapping:
        (original user_id, name) -> user_key

    Logic matches step_customer_management_ingest:
    - For each original user_id group:
        sort by creation_date ascending (NaT last)
        assign seq = 1..N
        user_key = <digits>-<seq>
          e.g., USER012195 group of 2 ->
                012195-1, 012195-2
          non-dupe USER61846 ->
                61846-1
    """
    ud = pd.read_csv(user_data_path)

    ud["user_id"] = ud["user_id"].astype(str)
    ud["name"] = ud["name"].astype(str)
    ud["creation_date"] = pd.to_datetime(ud["creation_date"], errors="coerce")

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("creation_date", na_position="last").copy()
        base_uid = group["user_id"].iloc[0]
        digits = _digits_from_user_id(base_uid)

        group["seq"] = range(1, len(group) + 1)
        group["user_key"] = [f"{digits}-{i}" for i in group["seq"]]

        return group[["user_id", "name", "user_key"]]

    mapped = ud.groupby("user_id", group_keys=False).apply(process_group)

    # Rename for merge clarity
    mapped = mapped.rename(columns={"user_id": "user_id_orig"})
    # Avoid accidental duplicates
    mapped = mapped.drop_duplicates(
        subset=["user_id_orig", "name", "user_key"])

    return mapped


def main():
    print(f"Reading user_credit_card: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # Drop auto-generated index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize columns
    if "user_id" in df.columns:
        df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["credit_card_number"] = df["credit_card_number"].astype(str)
    df["issuing_bank"] = df["issuing_bank"].astype(str)

    # ---------- Attach user_key based on user_data ---------- #
    print(f"Reading user_data for user_key mapping: {USER_DATA_FILE}")
    mapping = build_user_key_mapping_from_user_data(USER_DATA_FILE)

    if not mapping.empty:
        df = df.merge(
            mapping,
            left_on=["user_id", "name"],
            right_on=["user_id_orig", "name"],
            how="left",
        )

        num_with_key = df["user_key"].notna().sum()
        print(f"Assigned user_key for {num_with_key} user_credit_card rows.")

        # Drop helper ID columns (no more user_id in final output)
        drop_cols = [c for c in ["user_id", "user_id_orig"] if c in df.columns]
        df = df.drop(columns=drop_cols)
    else:
        print("No mapping built from user_data; user_key not assigned.")
        # If you want to enforce user_key presence, you can raise an error here.
    # --------------------------------------------------------- #

    # ---------- Reorder columns: user_key first ---------- #
    if "user_key" in df.columns:
        cols = ["user_key"] + [c for c in df.columns if c != "user_key"]
        df = df[cols]
    # ------------------------------------------------------ #

    # Save cleaned file
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved cleaned user_credit_card to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
