from pathlib import Path

# Raw IBM HI-Medium files. They stay outside the Git repository.
ACCOUNTS_FILE = Path(
    r"C:\Users\ruhan\OneDrive\Desktop\HI-Medium_accounts.csv"
)

TRANSACTIONS_FILE = Path(
    r"C:\Users\ruhan\OneDrive\Desktop\HI-Medium_Trans.csv"
)

# AML-Training-main directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Pipeline outputs will be created here.
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"