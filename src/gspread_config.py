"""Google Sheets configuration and uploader utility.

Contains a `GoogleSheetsUploader` class and environment-backed configuration
variables to authenticate and upload DataFrames to Google Sheets using a
service account.
"""
from typing import Optional
import os
import logging

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv

load_dotenv()

def _resolve_env_var(names):
    """Return first non-empty environment variable from the provided list, with surrounding quotes stripped."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v.strip().strip("'\"")
    return ""


# Read configuration from environment with fallbacks and sanitize values
GOOGLE_SERVICE_ACCOUNT_JSON: str = _resolve_env_var(["GOOGLE_SERVICE_ACCOUNT_JSON", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_SERVICE_ACCOUNT_CREDENTIALS"])  # path to json
GOOGLE_SPREADSHEET_ID: str = _resolve_env_var(["GOOGLE_SPREADSHEET_ID", "GOOGLE_SHEET_ID", "SPREADSHEET_ID"])  # spreadsheet id or name

# Optional: sheet names to upload to (defaults)
SHEET_NAME_FACT: str = _resolve_env_var(["SHEET_NAME_FACT", "SHEET_FACT", "FACT_SHEET_NAME"]) or "f_vendas"
SHEET_NAME_DIM: str = _resolve_env_var(["SHEET_NAME_DIM", "SHEET_DIM", "DIM_SHEET_NAME"]) or "d_produtos"


class GoogleSheetsUploader:
    """Utility class to upload Pandas DataFrames to Google Sheets.

    The class authenticates with a Google service account JSON file and uploads
    DataFrames using `set_with_dataframe`. The target worksheet is cleared and
    overwritten on each upload.
    """

    def __init__(self, credentials_path: str, spreadsheet_id: str, logger: Optional[logging.Logger] = None):
        """Initialize and authenticate the Google Sheets client.

        Args:
            credentials_path: Path to the service account JSON credentials file.
            spreadsheet_id: Spreadsheet ID (preferred) or spreadsheet name.
            logger: Optional logger. If not provided, the module logger is used.
        """
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.logger = logger or logging.getLogger("nomad_etl")
        self.client = None
        self.spreadsheet = None

        try:
            self.logger.info("Authenticating to Google Sheets using %s", credentials_path)
            if not os.path.exists(credentials_path):
                self.logger.error("Credentials file does not exist: %s", credentials_path)
                raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

            self.client = gspread.service_account(filename=credentials_path)

            try:
                self.spreadsheet = self.client.open_by_key(spreadsheet_id)
            except Exception:
                # fallback to open by name
                self.spreadsheet = self.client.open(spreadsheet_id)

            # test access: list worksheets
            try:
                ws = [w.title for w in self.spreadsheet.worksheets()]
                self.logger.info("Opened spreadsheet: %s (worksheets: %s)", spreadsheet_id, ws)
            except Exception:
                self.logger.info("Opened spreadsheet: %s (could not list worksheets)", spreadsheet_id)
        except Exception as exc:
            self.logger.error("Authentication/opening spreadsheet failed: %s", exc)
            raise

    def upload_dataframe(self, df: pd.DataFrame, sheet_name: str):
        """Upload a DataFrame to a worksheet.

        The worksheet is cleared if present or created otherwise. Datetime
        columns are converted to ISO strings for compatibility with Google Sheets.

        Args:
            df: DataFrame to upload. Column names are expected to be localized to Portuguese.
            sheet_name: Target worksheet name.
        """
        self.logger.info("Uploading DataFrame to sheet '%s' (rows=%d)", sheet_name, len(df))
        try:
            payload = df.copy()
            # Convert datetimes to strings
            for col in payload.select_dtypes(include=["datetime64[ns]", "datetimetz"]):
                payload[col] = payload[col].dt.strftime("%Y-%m-%d %H:%M:%S")

            # Replace NaN with None for Sheets
            payload = payload.where(pd.notnull(payload), None)

            try:
                worksheet = self.spreadsheet.worksheet(sheet_name)
                worksheet.clear()
            except Exception:
                rows = max(1, len(payload) + 1)
                cols = max(1, len(payload.columns))
                worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows=str(rows), cols=str(cols))

            set_with_dataframe(worksheet, payload, include_index=False)
            self.logger.info("Successfully uploaded DataFrame to sheet '%s'", sheet_name)
        except Exception as exc:
            self.logger.error("Failed to upload DataFrame to sheet '%s': %s", sheet_name, exc)
            raise
