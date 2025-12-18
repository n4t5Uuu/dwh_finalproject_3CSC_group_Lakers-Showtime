import os
import pandas as pd
from pathlib import Path


# CONFIG
ROOT_DIR = Path("/data_files")

SUPPORTED_EXTENSIONS = {
    ".html",
    ".json",
    ".parquet",
    ".xlsx",
    ".pickle",
}


# FILE CONVERTERS
def convert_to_csv(file_path: str):
    ext = os.path.splitext(file_path)[1].lower()
    csv_path = os.path.splitext(file_path)[0] + ".csv"

    if os.path.exists(csv_path):
        src_mtime = os.path.getmtime(file_path)
        csv_mtime = os.path.getmtime(csv_path)
        if csv_mtime >= src_mtime:
            print(f"[SKIP] Up-to-date: {file_path}")
            return

    try:
        if ext == ".html":
            dfs = pd.read_html(file_path)
            if not dfs:
                return
            df = dfs[0]

        elif ext == ".json":
            df = pd.read_json(file_path)

        elif ext == ".parquet":
            df = pd.read_parquet(file_path)

        elif ext == ".xlsx":
            df = pd.read_excel(file_path)

        elif ext == ".pickle":
            df = pd.read_pickle(file_path)

        else:
            return

        df.to_csv(csv_path, index=False)
        print(f"[OK] Converted: {file_path} → {csv_path}")

    except Exception as e:
        print(f"[ERROR] Failed to convert {file_path}: {e}")


def process_directory(root_dir: str):
    for root, _, files in os.walk(root_dir):
        for file in files:
            ext = os.path.splitext(file)[1].lower()

            # Skip CSVs completely
            if ext == ".csv":
                continue

            if ext in SUPPORTED_EXTENSIONS:
                file_path = os.path.join(root, file)
                convert_to_csv(file_path)


def main():
    print(f"Starting CSV conversion in {ROOT_DIR}")
    process_directory(ROOT_DIR)
    print("CSV conversion complete.")
