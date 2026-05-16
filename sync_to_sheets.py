import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================

SERVICE_ACCOUNT_FILE = "credentials/service_account.json"
CSV_FILE = "job_database.csv"

SPREADSHEET_ID = "1JNUiBxUJL43Mmn9d8OQ2O0D3TZifsE-9_beMIxmZ394"
SHEET_NAME = "Bot_Job_Leads"

# =========================
# AUTH
# =========================

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=scopes
)

client = gspread.authorize(creds)

# =========================
# READ CSV
# =========================

df = pd.read_csv(CSV_FILE)

# Convert all values to string-safe format for Google Sheets
df = df.fillna("").astype(str)

# =========================
# OPEN GOOGLE SHEET
# =========================

spreadsheet = client.open_by_key(SPREADSHEET_ID)
worksheet = spreadsheet.worksheet(SHEET_NAME)

print("SUCCESS: Connected to Google Sheet")
print(f"CSV rows found: {len(df)}")

# =========================
# CLEAR OLD DATA
# =========================

worksheet.clear()

# =========================
# WRITE NEW DATA
# =========================

data = [df.columns.tolist()] + df.values.tolist()

worksheet.update(
    values=data,
    range_name="A1"
)

print("SUCCESS: job_database.csv synced to Bot_Job_Leads")
print(f"Rows uploaded: {len(df)}")

df = pd.read_csv(CSV_FILE)