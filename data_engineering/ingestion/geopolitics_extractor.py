# data_engineering/ingestion/geopolitics_extractor.py
from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from pathlib import Path
import pandas as pd
import os

import dotenv
dotenv.load_dotenv()

class ACLEDDataExtractor:
    """
    Interacts with the HDX API to pull ACLED conflict datasets for Nigeria,
    transforming tabular data into Markdown for LLM comprehension.
    """
    def __init__(self, api_key: str, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        
        setup_logging()
        # Initialize HDX Configuration
        Configuration.create(hdx_site='prod', user_agent='RAG_Ingestion_Pipeline', hdx_key=api_key)

    def download_nigeria_conflict_data(self):
        print("Querying HDX API for ACLED Nigeria Conflict Data...")
        # Targeting the specific dataset identifier for Nigeria ACLED data
        dataset = Dataset.read_from_hdx('nigeria-acled-conflict-data')
        
        if not dataset:
            print("Failed to locate dataset on HDX.")
            return

        resources = dataset.get_resources()
        
        for resource in resources:
            # Target structured data files
            if 'xlsx' in resource['format'].lower() or 'csv' in resource['format'].lower():
                file_name = resource['name']
                print(f"Downloading ACLED resource: {file_name}")
                
                target_path = os.path.join(self.storage_dir, file_name)
                # HDX library handles the download stream automatically
                url, path = resource.download(self.storage_dir)
                path = Path(path)

                # Transform to Markdown for optimal LLM context window ingestion.
                # ACLED HRP workbooks usually contain a licensing sheet and one or more
                # aggregated data sheets (eg. events & fatalities by month-year).
                if path.suffix == ".xlsx":
                    # Load all sheets so we can skip pure licensing sheets.
                    sheets = pd.read_excel(path, sheet_name=None)
                    data_df = None
                    for sheet_name, df in sheets.items():
                        # Heuristic: skip sheets whose first column is "Licensing"
                        first_col = str(df.columns[0]).strip().lower() if len(df.columns) > 0 else ""
                        if "licensing" in first_col:
                            continue
                        # Prefer sheet that has a month/year column
                        col_str = " ".join(map(str, df.columns)).lower()
                        if "month-year" in col_str or "month_year" in col_str or "month" in col_str:
                            data_df = df
                            break
                    if data_df is None:
                        # Fallback: pick the first non-licensing sheet if any
                        for sheet_name, df in sheets.items():
                            first_col = str(df.columns[0]).strip().lower() if len(df.columns) > 0 else ""
                            if "licensing" not in first_col:
                                data_df = df
                                break
                    if data_df is None:
                        print(f"No suitable data sheet found in {path.name}; skipping markdown generation.")
                        continue

                    # Drop completely empty rows and keep only the most recent rows to stay concise.
                    data_df = data_df.dropna(how="all")
                    if "Month-year" in data_df.columns:
                        # Put Month-year first if present.
                        cols = ["Month-year"] + [c for c in data_df.columns if c != "Month-year"]
                        data_df = data_df[cols]
                    recent = data_df.tail(24)  # last 24 rows (typically 2 years of monthly aggregates)

                    md_table = recent.to_markdown(index=False)
                    md_path = path.with_suffix(".md")

                    with open(md_path, 'w', encoding='utf-8') as f:
                        f.write("# ACLED Nigeria Conflict Summary (HRP aggregates)\n\n")
                        f.write("> Source: ACLED via HDX. See ACLED Terms of Use before external redistribution.\n\n")
                        f.write(md_table)
                    print(f"Generated conflict trend markdown for RAG ingestion at {md_path}")
                    
if __name__ == "__main__":
    extractor = ACLEDDataExtractor(api_key=os.getenv("HDX_API_KEY"), storage_dir="data_engineering/ingestion/data/acled")
    extractor.download_nigeria_conflict_data()