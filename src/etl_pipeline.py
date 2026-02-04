"""ETL pipeline for Nomad Pizzaria case.

This module loads sales and pizza type data from Excel, transforms them into a
products dimension and a sales fact table, performs basic integrity checks,
and writes results to Parquet files in the `data/gold` folder. Logs are
written to `logs/{YYYY-MM-DD}_pipeline.log`.
"""
from pathlib import Path
from datetime import datetime
import logging
from typing import Tuple
import os
from dotenv import load_dotenv
import pandas as pd

from .gspread_config import GoogleSheetsUploader
from .gspread_config import (
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SPREADSHEET_ID,
    SHEET_NAME_FACT,
    SHEET_NAME_DIM,
)
from .utils import classify_time_of_day

load_dotenv() 

# Paths relative to project root
BASE_DIR = Path(__file__).resolve().parents[1]
# Sales input now reads from Excel file provided in the case
INPUT_PATH_SALES = BASE_DIR / "data" / "raw" / "Pizza_Sales.xlsx"
OUTPUT_PATH_FACT = BASE_DIR / "data" / "gold" / "f_vendas.parquet"
OUTPUT_PATH_DIM = BASE_DIR / "data" / "gold" / "d_produtos.parquet"
LOG_DIR = BASE_DIR / "logs"



def setup_logger(name: str = "nomad_etl") -> logging.Logger:
    """Configure and return a logger that writes to a daily file in `logs/`.

    Files follow the convention: YYYY-MM-DD_pipeline.log
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = LOG_DIR / f"{date_str}_pipeline.log"

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger

def load_data(path: Path, logger: logging.Logger = None, sheet_name: str = None) -> pd.DataFrame:
    """Load CSV or Excel files into a DataFrame.

    For Excel files, a specific sheet name may be provided. Returns an empty
    DataFrame on failure and logs errors instead of raising.
    """
    logger = logger or logging.getLogger("nomad_etl")
    if not path.exists():
        logger.error("Input file not found: %s", path)
        return pd.DataFrame()
    try:
        if str(path).lower().endswith('.csv'):
            return pd.read_csv(path)
        return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    except ValueError as exc:
        logger.error("Sheet '%s' not found in %s: %s", sheet_name, path, exc)
        return pd.DataFrame()
    except Exception as exc:
        logger.exception("Failed to load %s (sheet=%s): %s", path, sheet_name, exc)
        return pd.DataFrame()


def process_dimension_products(types_df: pd.DataFrame, logger: logging.Logger = None) -> pd.DataFrame:
    """Process product types into a dimension table with direct Portuguese translation.

    Schema Mapping:
    - pizza_type_id  -> id_tipo_pizza
    - pizza_name     -> nome_pizza
    - pizza_category -> categoria_pizza
    - pizza_ingredients -> ingredientes
    - (Derived)      -> contagem_ingredientes
    """
    logger = logger or logging.getLogger("nomad_etl")
    logger.info("Processing product dimension (d_produtos) with direct mapping")

    df = types_df.copy()

    direct_mapping = {
        "pizza_type_id": "id_tipo_pizza",
        "pizza_name": "nome_pizza",
        "pizza_category": "categoria_pizza",
        "pizza_ingredients": "ingredientes"
    }

    df = df.rename(columns=direct_mapping)

    required_cols = list(direct_mapping.values())
    for col in required_cols:
        if col not in df.columns:
            logger.error(f"Critical column missing after mapping: {col}")
            df[col] = pd.NA

    df["ingredientes"] = df["ingredientes"].fillna("").astype(str)
    
    df["contagem_ingredientes"] = df["ingredientes"].str.count(",") + 1
    df.loc[df["ingredientes"] == "", "contagem_ingredientes"] = 0
    df["contagem_ingredientes"] = df["contagem_ingredientes"].astype(int)

    try:
        df["categoria_pizza"] = df["categoria_pizza"].astype("category")
    except Exception:
        pass

    logger.info(f"Dimension 'd_produtos' processed: {df.shape[0]} rows.")

    return df[[
        "id_tipo_pizza", 
        "nome_pizza", 
        "categoria_pizza", 
        "ingredientes", 
        "contagem_ingredientes"
    ]]

def process_fact_sales(sales_df: pd.DataFrame, dim_df: pd.DataFrame, logger: logging.Logger = None) -> pd.DataFrame:   
    """
    Process sales into a rich fact table merging with product dimension.

    Produces columns:
    - id_pedido, id_tipo_pizza, quantidade, data_pedido, hora_pedido, preco_unitario_pizza, tamanho
    - nome_pizza, categoria_pizza, contagem_ingredientes (Enriched from Dim)
    - dia_da_semana, eh_final_de_semana, periodo_dia, valor_total_linha, valor_total_pedido
    """
    logger = logger or logging.getLogger("nomad_etl")
    logger.info("Processing sales fact (f_vendas) with data cleaning and enrichment")

    df = sales_df.copy()

    # 1. Standardize Column Names
    direct_mapping = {
        "order_id": "id_pedido",
        "pizza_name_id": "id_tipo_pizza",
        "quantity": "quantidade",
        "order_date": "data_pedido",
        "order_time": "hora_pedido",
        "unit_price": "preco_unitario_pizza",
        "pizza_size": "tamanho"
    }
    df = df.rename(columns=direct_mapping)

    # 2. ID Normalization Logic
    valid_pizza_types = [
        "bbq_ckn", "big_meat", "brie_carre", "calabrese", "cali_ckn", "ckn_alfredo",
        "ckn_pesto", "classic_dlx", "five_cheese", "four_cheese", "green_garden",
        "hawaiian", "ital_cpcllo", "ital_supr", "ital_veggie", "mediterraneo",
        "mexicana", "napolitana", "pep_msh_pep", "pepperoni", "peppr_salami",
        "prsc_argla", "sicilian", "soppressata", "southw_ckn", "spicy_ital",
        "spin_pesto", "spinach_fet", "spinach_supr", "thai_ckn", "the_greek", "veggie_veg"
    ]

    def clean_pizza_id(pid):
        pid_str = str(pid).lower()
        for valid_id in valid_pizza_types:
            if pid_str.startswith(valid_id):
                return valid_id
        return pid

    df["id_tipo_pizza"] = df["id_tipo_pizza"].apply(clean_pizza_id)

    # 3. Data Typing & Cleaning
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0).astype(int)
    df["preco_unitario_pizza"] = pd.to_numeric(df["preco_unitario_pizza"], errors="coerce").fillna(0.0)
    df["data_pedido"] = pd.to_datetime(df["data_pedido"], errors="coerce")

    # Handle Time logic
    df["data_pedido"] = pd.to_datetime(df["data_pedido"])

    if df["hora_pedido"].isnull().all() and df["data_pedido"].notnull().any():
        df["hora_formatada"] = df["data_pedido"].dt.hour 
        df["hora_pedido"] = df["data_pedido"].dt.strftime('%H:%M')

    else:
        temp_hora = pd.to_datetime(df["hora_pedido"], format='%H:%M:%S', errors='coerce')
        df["hora_formatada"] = temp_hora.dt.hour
        df["hora_pedido"] = temp_hora.dt.strftime('%H:%M')
        
    # 4. Temporal Features
    day_map = {
        "Monday": "Segunda-feira", "Tuesday": "Terça-feira", "Wednesday": "Quarta-feira",
        "Thursday": "Quinta-feira", "Friday": "Sexta-feira", "Saturday": "Sábado", "Sunday": "Domingo"
    }
    df["dia_da_semana"] = df["data_pedido"].dt.day_name().map(day_map).fillna("")
    df["eh_final_de_semana"] = df["data_pedido"].dt.dayofweek.isin([5, 6])
    
    # Assumes classify_time_of_day is defined in the scope or imported
    df["periodo_dia"] = df["hora_pedido"].apply(lambda x: classify_time_of_day(x))
    
    # 5. Financial Metrics Calculation
    df["valor_total_linha"] = df["quantidade"] * df["preco_unitario_pizza"]
    totals = df.groupby("id_pedido")["valor_total_linha"].transform("sum")
    df["valor_total_pedido"] = totals

    # ---------------------------------------------------------
    # 6. ENRICHMENT LAYER (The Join Strategy)
    # ---------------------------------------------------------
    logger.info("Starting enrichment: Merging Fact Sales with Dim Products")

    # Prepare Dim Table Subset to ensure clean join
    # Assuming dim_df has 'id_tipoF_pizza' as PK
    cols_to_use = ["id_tipo_pizza", "nome_pizza", "categoria_pizza", "contagem_ingredientes"]
    
    # Validate if columns exist in dim_df before merging to avoid KeyErrors
    available_cols = [c for c in cols_to_use if c in dim_df.columns]
    dim_subset = dim_df[available_cols].copy()

    # Perform Left Join (Keep all sales, attach product info where possible)
    merged_df = df.merge(
        dim_subset,
        left_on="id_tipo_pizza",
        right_on="id_tipo_pizza",
        how="left"
    )

    # ---------------------------------------------------------
    # 7. OBSERVABILITY & QUALITY CHECKS
    # ---------------------------------------------------------
    total_rows = len(merged_df)
    missing_matches = merged_df["nome_pizza"].isnull().sum()
    
    if missing_matches > 0:
        logger.warning(f"Data Quality Alert: {missing_matches} sales rows ({missing_matches/total_rows:.1%}) failed to match with a product in Dim Table.")
        
        # Fallback values to ensure Looker doesn't break
        merged_df["nome_pizza"] = merged_df["nome_pizza"].fillna("Desconhecido")
        merged_df["categoria_pizza"] = merged_df["categoria_pizza"].fillna("Outros")
        merged_df["contagem_ingredientes"] = merged_df["contagem_ingredientes"].fillna(0).astype(int)
    else:
        logger.info("Enrichment successful: 100% match rate between Sales and Products.")

    logger.info(f"Final processed dataset shape: {merged_df.shape}")

    # 8. Final Column Selection
    return merged_df[
        [
            "id_pedido", 
            "id_tipo_pizza", 
            "nome_pizza",
            "categoria_pizza",      
            "contagem_ingredientes",
            "quantidade", 
            "data_pedido", 
            "hora_pedido",
            "hora_formatada",
            "preco_unitario_pizza", 
            "tamanho", 
            "dia_da_semana", 
            "eh_final_de_semana", 
            "periodo_dia", 
            "valor_total_linha", 
            "valor_total_pedido"
        ]
    ]

def run_pipeline(logger: logging.Logger) -> Tuple[Path, Path]:
    """Execute the ETL pipeline and return paths to saved Parquet files.

    The function logs progress and warnings for data integrity issues.
    """
    logger.info("Starting ETL pipeline")

    # Read the 'pizza_sales' and 'pizzas' sheets from the provided Excel workbook
    sales_df = load_data(INPUT_PATH_SALES, logger, sheet_name="pizza_sales")
    types_df = load_data(INPUT_PATH_SALES, logger, sheet_name="pizzas")

    if sales_df.empty:
        logger.error("Sales sheet 'pizza_sales' is missing or empty in %s", INPUT_PATH_SALES)
    if types_df.empty:
        logger.error("Types sheet 'pizzas' is missing or empty in %s", INPUT_PATH_SALES)
    if sales_df.empty or types_df.empty:
        logger.error("Aborting pipeline due to missing sheets in the workbook.")
        return None, None

    logger.info("Loaded sheets: 'pizza_sales'=%d rows, 'pizzas'=%d rows", sales_df.shape[0], types_df.shape[0])

    dim_products = process_dimension_products(types_df, logger)
    fact_sales = process_fact_sales(sales_df, dim_products, logger)

    # Integrity check: IDs referenced in sales must exist in types
    sales_ids = set(fact_sales["id_tipo_pizza"].dropna().unique())
    type_ids = set(dim_products["id_tipo_pizza"].dropna().unique())
    missing_ids = sorted(list(sales_ids - type_ids))
    if missing_ids:
        sample_missing = missing_ids[:10]
        logger.warning("Sales reference pizza types missing in types lookup: %s (showing up to 10 of %d)", sample_missing, len(missing_ids))

    # Ensure output directories exist
    OUTPUT_PATH_FACT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH_DIM.parent.mkdir(parents=True, exist_ok=True)

    # Save Parquet files
    fact_sales.to_parquet(OUTPUT_PATH_FACT, index=False, engine="pyarrow")
    dim_products.to_parquet(OUTPUT_PATH_DIM, index=False, engine="pyarrow")

    logger.info("Saved fact table to %s", OUTPUT_PATH_FACT)
    logger.info("Saved dimension table to %s", OUTPUT_PATH_DIM)

    # Save in Big Query
    logger.info("uploading data to BigQuery.")
    project_id = os.getenv("projeto_id")
    dataset_id = os.getenv("dataset_id")
    sales_table_id = f"{dataset_id}.f_vendas"
    product_table_id = f"{dataset_id}.d_produtos"

    fact_sales.to_gbq(
        destination_table=sales_table_id, 
        project_id=project_id, 
        if_exists='replace'
    )
    dim_products.to_gbq(
        destination_table=product_table_id, 
        project_id=project_id, 
        if_exists='replace'
    )
    logger.info("SUCCESS uploading data to BigQuery.")
    
    # Optionally upload to Google Sheets if credentials and spreadsheet id are provided
    if GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_SPREADSHEET_ID:
        try:
            logger.info("Google Sheets credentials and spreadsheet ID found. Initializing uploader.")
            uploader = GoogleSheetsUploader(GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SPREADSHEET_ID, logger=logger)

            logger.info("Starting upload of dimension table to Google Sheets (sheet: %s)", SHEET_NAME_DIM)
            uploader.upload_dataframe(dim_products, SHEET_NAME_DIM)

            logger.info("Starting upload of fact table to Google Sheets (sheet: %s)", SHEET_NAME_FACT)
            uploader.upload_dataframe(fact_sales, SHEET_NAME_FACT)

            logger.info("Google Sheets upload completed successfully")
        except Exception:
            logger.exception("Google Sheets upload failed")
    else:
        logger.info("Google Sheets credentials or Spreadsheet ID not provided; skipping upload")

    logger.info("ETL pipeline completed successfully")
    return OUTPUT_PATH_FACT, OUTPUT_PATH_DIM


if __name__ == "__main__":
    logger = setup_logger()
    try:
        run_pipeline(logger)
    except Exception as e:
        logger.exception(f"ETL pipeline failed: {e}")
        raise
