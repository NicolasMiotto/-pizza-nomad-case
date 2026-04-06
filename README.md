# Nomad Pizzaria Case

Project structure and ETL pipeline for Nomad Bank technical case.

- Data: raw Excel files -> transformed to Parquet (gold)
- ETL implemented in `src/etl_pipeline.py`
- Requirements in `requirements.txt`

## Run instructions (PowerShell and Bash) ✅

**Prerequisites**
- Create and activate a Python virtual environment, then install dependencies:
  - PowerShell:

    ```powershell
    python -m venv .venv
    .\.venv\Scripts\Activate
    pip install -r requirements.txt
    ```

  - Bash / macOS:

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

**Environment variables**
- Create a `.env` from `.env.template` or set variables directly in your shell.
- Required variables (examples):
  - `GOOGLE_SERVICE_ACCOUNT_JSON` — path to the Google service account JSON file
  - `GOOGLE_SPREADSHEET_ID` — spreadsheet ID or spreadsheet name

**PowerShell example (session variables):**

```powershell
$env:GOOGLE_SERVICE_ACCOUNT_JSON = "C:\path\to\service-account.json"
$env:GOOGLE_SPREADSHEET_ID = "your-spreadsheet-id"
python -m src.etl_pipeline
```

**Bash example (session variables):**

```bash
export GOOGLE_SERVICE_ACCOUNT_JSON="/path/to/service-account.json"
export GOOGLE_SPREADSHEET_ID="your-spreadsheet-id"
python -m src.etl_pipeline
```

**Notes & troubleshooting**
- Run the pipeline from the project root **using the module form** (`python -m src.etl_pipeline`) to allow relative imports.
- Logs are written to `logs/YYYY-MM-DD_pipeline.log` and outputs are saved in `data/gold/`.
- To skip Google Sheets upload, leave `GOOGLE_*` variables unset.
- If you still get `ImportError: attempted relative import with no known parent package`, ensure you are executing with `-m` from the project root.

---

If you want, I can add a `Makefile` or shell/PowerShell wrapper to simplify these commands.
