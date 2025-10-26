import polars as pl
from pathlib import Path
from datetime import datetime

def convert_csv_to_parquet(input_folder: str, output_base_path: str) -> None:
    """
    Minimalist CSV to Parquet converter with encoding detection.
    """
    separator = ";"
    output_folder = Path(output_base_path) / f"PARQUET_{datetime.now().strftime('%Y%m%d')} {Path(input_folder).name}"
    output_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"🔄 Converting: {input_folder} -> {output_folder}")
    
    for file_path in Path(input_folder).glob('*.csv'):
        try:
            # Try different encodings
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pl.read_csv(
                        file_path, 
                        separator=separator,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                        infer_schema_length=100000,
                        encoding=encoding
                    )
                    break
                except Exception:
                    continue
            
            if df is None:
                print(f"❌ {file_path.name}: Could not read with any encoding")
                continue
                
            df.write_parquet(output_folder / f"{file_path.stem}.parquet")
            print(f"✅ {file_path.name} -> {len(df):,} rows, {len(df.columns)} cols")
            
        except Exception as e:
            print(f"❌ {file_path.name}: {e}")
    
    print("🎉 Conversion completed")