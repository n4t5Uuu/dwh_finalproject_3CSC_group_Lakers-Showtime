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
        "facts"
    ]

    # Create base folder
    BASE.mkdir(parents=True, exist_ok=True)

    # Create each department folder
    for folder in dept_folders:
        path = BASE / folder
        path.mkdir(parents=True, exist_ok=True)

        print(f"Folder exists/created: {path}")

    print("\nDirectory structure ready.\n")


if __name__ == "__main__":
    main()
