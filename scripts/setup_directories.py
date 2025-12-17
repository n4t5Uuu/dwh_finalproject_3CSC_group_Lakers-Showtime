import os
from pathlib import Path



def main():


    BASE = Path("/clean_data")
    dept_folders = [
        "business",
        "customer_management",
        "enterprise",
        "marketing",
        "operations",
        "facts",
        "dimensions",
    ]

    print("\n[SETUP] Ensuring clean_data folder structure exists...\n")

    # Create base folder
    BASE.mkdir(parents=True, exist_ok=True)

    # Create each department folder
    for folder in dept_folders:
        path = BASE / folder
        path.mkdir(parents=True, exist_ok=True)

        print(f"[OK] Folder exists/created: {path}")

    print("\n[SETUP COMPLETE] Directory structure ready.\n")


if __name__ == "__main__":
    main()
