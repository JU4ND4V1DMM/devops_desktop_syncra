import pandas.errors as pd_errors
import pandas as pd
import os
from openpyxl import load_workbook
import polars as pl
from pathlib import Path

def search_values_in_files(directory, output_path, search_list, process_data):
    search_values = [v.strip() for v in search_list.split(',')]
    print(f"🔎 Searching for values: {search_values}")
    
    datetime_now = pd.Timestamp.now().strftime('%Y%m%d - %H%M')
    output_excel = os.path.join(output_path, f'Search Results {datetime_now}.xlsx')
    
    results = {}
    print("📂 Searching through CSV, Excel and Parquet files...")

    # Improved CSV function with better type handling
    def read_csv_dynamic(file_path, delimiter=';'):
        try:
            # First try with dtype as string to avoid mixed type issues
            return pd.read_csv(file_path, delimiter=delimiter, encoding='utf-8', dtype=str, low_memory=False)
        except UnicodeDecodeError:
            print(f"⚠️ Encoding issue in {file_path}. Retrying with 'latin-1'...")
            try:
                return pd.read_csv(file_path, delimiter=delimiter, encoding='latin-1', dtype=str, low_memory=False)
            except Exception as e:
                print(f"❌ Still failed with 'latin-1': {e}")
                return None
        except pd_errors.EmptyDataError:
            print(f"⚠️ Empty file: {file_path}")
            return None
        except pd_errors.ParserError as e:
            print(f"❌ Parsing error in {file_path}: {e}")
            try:
                print(f"⚠️ Trying to skip bad lines in {file_path}...")
                return pd.read_csv(file_path, delimiter=delimiter, encoding='utf-8', on_bad_lines='skip', dtype=str, low_memory=False)
            except Exception as e2:
                print(f"❌ Could not recover from parse error: {e2}")
                return None
        except Exception as e:
            print(f"❌ Unexpected error in {file_path}: {e}")
            return None

    # Optimized CSV reading with Polars and better error handling
    def read_csv_polars_optimized(file_path, delimiter=';'):
        """Optimized CSV reading with Polars for large files"""
        try:
            # Read with null values and infer schema more safely
            df_pl = pl.read_csv(
                file_path, 
                separator=delimiter, 
                infer_schema_length=10000,
                null_values=["", "NULL", "null", "NaN", "nan"],
                try_parse_dates=False
            )
            return df_pl
        except Exception as e:
            print(f"❌ Polars failed for {file_path}: {e}. Falling back to pandas...")
            df_pd = read_csv_dynamic(file_path, delimiter)
            if df_pd is not None:
                try:
                    return pl.from_pandas(df_pd)
                except Exception as pl_error:
                    print(f"❌ Could not convert pandas to polars: {pl_error}. Using manual conversion...")
                    return convert_dataframe_safely(df_pd)
            return None

    def convert_dataframe_safely(df_pd):
        """Safely convert pandas DataFrame to Polars handling any data type issues"""
        try:
            # Ensure all columns are string type to avoid conversion issues
            df_pd = df_pd.astype(str)
            # Replace NaN and None with empty strings
            df_pd = df_pd.fillna('')
            return pl.from_pandas(df_pd)
        except Exception as e:
            print(f"❌ Safe conversion failed: {e}. Creating new DataFrame...")
            # Last resort: create new DataFrame with only string columns
            columns = df_pd.columns.tolist()
            data = {}
            for col in columns:
                data[col] = df_pd[col].astype(str).fillna('').tolist()
            return pl.DataFrame(data)

    # Optimized Parquet reading
    def read_parquet_optimized(file_path):
        """Ultra-fast Parquet file reading"""
        try:
            df_pl = pl.read_parquet(file_path)
            print(f"⚡ Parquet file loaded: {os.path.basename(file_path)} - Shape: {df_pl.shape}")
            return df_pl
        except Exception as e:
            print(f"❌ Error reading Parquet file {file_path}: {e}")
            return None

    def search_in_dataframe_polars(df_pl, value, filename, sheet_name="N/A"):
        """Optimized search with Polars - CORRECTED VERSION"""
        try:
            # Convert all columns to string for consistent searching
            df_str = df_pl.cast(pl.Utf8, strict=False).fill_null("")
            
            # CORRECTED: Build mask using proper Polars expressions
            mask = pl.lit(False)
            for col in df_str.columns:
                # Use proper Polars expression syntax
                col_mask = df_str[col].str.contains(value)
                mask = mask | col_mask  # Use | operator for Polars expressions
            
            # Apply the mask to filter rows
            matching_rows = df_str.filter(mask)
            
            if len(matching_rows) > 0:
                # Add metadata columns
                matching_rows = matching_rows.with_columns([
                    pl.lit(filename).alias("Source_File"),
                    pl.lit(sheet_name).alias("Source_Sheet")
                ])
                return matching_rows
            return None
            
        except Exception as e:
            print(f"❌ Search error in {filename}: {e}")
            # Fallback to alternative search method
            return search_in_dataframe_fallback(df_pl, value, filename, sheet_name)

    def search_in_dataframe_fallback(df_pl, value, filename, sheet_name="N/A"):
        """Alternative search method using different approach"""
        try:
            # Convert to string and handle nulls
            df_str = df_pl.cast(pl.Utf8, strict=False).fill_null("")
            
            # Use fold to combine column masks properly
            mask = pl.fold(
                acc=pl.lit(False),
                function=lambda acc, col: acc | col.str.contains(value),
                exprs=[pl.col(col) for col in df_str.columns]
            )
            
            matching_rows = df_str.filter(mask)
            
            if len(matching_rows) > 0:
                matching_rows = matching_rows.with_columns([
                    pl.lit(filename).alias("Source_File"),
                    pl.lit(sheet_name).alias("Source_Sheet")
                ])
                return matching_rows
            return None
            
        except Exception as e:
            print(f"❌ Fallback search also failed for {filename}: {e}")
            return search_in_dataframe_simple(df_pl, value, filename, sheet_name)

    def search_in_dataframe_simple(df_pl, value, filename, sheet_name="N/A"):
        """Simple row-by-row search as last resort"""
        try:
            # Convert to pandas and search (less efficient but reliable)
            df_pd = df_pl.to_pandas()
            df_pd = df_pd.astype(str).fillna('')
            
            # Search across all columns
            mask = df_pd.apply(lambda row: any(value in str(cell) for cell in row), axis=1)
            matching_rows = df_pd[mask].copy()
            
            if len(matching_rows) > 0:
                matching_rows['Source_File'] = filename
                matching_rows['Source_Sheet'] = sheet_name
                return pl.from_pandas(matching_rows)
            return None
            
        except Exception as e:
            print(f"❌ Simple search failed for {filename}: {e}")
            return None

    def search_in_dataframe_safe(df, value, filename, sheet_name="N/A"):
        """Fallback search method when Polars fails"""
        try:
            # Convert to pandas if it's a Polars DataFrame
            if hasattr(df, 'to_pandas'):
                df_pd = df.to_pandas()
            else:
                df_pd = df
            
            # Ensure all data is string for searching
            df_pd = df_pd.astype(str).fillna('')
            
            # Search across all columns - look for partial matches
            mask = df_pd.apply(lambda row: any(value in str(cell) for cell in row), axis=1)
            matching_rows = df_pd[mask].copy()
            
            if len(matching_rows) > 0:
                matching_rows['Source_File'] = filename
                matching_rows['Source_Sheet'] = sheet_name
                return pl.from_pandas(matching_rows)
            return None
        except Exception as e:
            print(f"❌ Fallback search failed for {filename}: {e}")
            return None

    # --- Search CSV Files OPTIMIZED ---
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    print(f"📊 Processing {len(csv_files)} CSV files with Polars optimization...")
    
    for filename in csv_files:
        file_path = os.path.join(directory, filename)
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        
        # Use Polars for large files, pandas for small ones
        if file_size > 50:  # Files > 50MB
            print(f"🚀 Using Polars for large CSV: {filename} ({file_size:.1f} MB)")
            df = read_csv_polars_optimized(file_path)
        else:
            df_pd = read_csv_dynamic(file_path)
            df = pl.from_pandas(df_pd) if df_pd is not None else None
        
        if df is None:
            print(f"⛔ Skipping unreadable CSV: {filename}")
            continue
        
        for value in search_values:
            # Try optimized search first
            matching_rows = search_in_dataframe_polars(df, value, filename, "N/A")
            
            if matching_rows is not None and len(matching_rows) > 0:
                print(f"✅ Value '{value}' found in CSV: {filename} ({len(matching_rows)} matches)")
                
                if value not in results:
                    results[value] = []
                results[value].append(matching_rows)

    # --- Search Parquet Files ULTRA FAST ---
    parquet_files = [f for f in os.listdir(directory) if f.endswith('.parquet')]
    print(f"⚡ Processing {len(parquet_files)} Parquet files (ultra-fast)...")
    
    for filename in parquet_files:
        file_path = os.path.join(directory, filename)
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        
        print(f"🚀 Processing Parquet: {filename} ({file_size:.1f} MB)")
        df = read_parquet_optimized(file_path)
        
        if df is None:
            print(f"⛔ Skipping unreadable Parquet: {filename}")
            continue
        
        for value in search_values:
            matching_rows = search_in_dataframe_polars(df, value, filename, "Parquet")
            if matching_rows is not None and len(matching_rows) > 0:
                print(f"✅ Value '{value}' found in Parquet: {filename} ({len(matching_rows)} matches)")
                
                if value not in results:
                    results[value] = []
                results[value].append(matching_rows)

    # --- Search Excel Files (keeping pandas for Excel) ---
    excel_files = [f for f in os.listdir(directory) if f.endswith(('.xlsx', '.xls'))]
    print(f"📊 Processing {len(excel_files)} Excel files...")
    
    for filename in excel_files:
        file_path = os.path.join(directory, filename)
        
        try:
            xls = pd.ExcelFile(file_path)
            sheet_names = xls.sheet_names
        except Exception as e:
            print(f"❌ Could not read Excel file {filename}: {e}")
            continue

        for sheet_name in sheet_names:
            try:
                # Read as string to avoid type issues
                df_pd = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
                df_pd = df_pd.fillna('')
                df = pl.from_pandas(df_pd)
            except Exception as e:
                print(f"❌ Could not read sheet '{sheet_name}' from {filename}: {e}")
                continue

            for value in search_values:
                matching_rows = search_in_dataframe_polars(df, value, filename, sheet_name)
                if matching_rows is not None and len(matching_rows) > 0:
                    print(f"✅ Value '{value}' found in Excel: {filename} (Sheet: {sheet_name}) - {len(matching_rows)} matches")
                    
                    if value not in results:
                        results[value] = []
                    results[value].append(matching_rows)

    # Search summary
    print("\n📈 SEARCH SUMMARY:")
    print("=" * 50)
    for value in search_values:
        if value in results:
            total_matches = sum(len(df) for df in results[value])
            print(f"✅ '{value}': {total_matches} total matches")
        else:
            print(f"❌ '{value}': No matches found")
    print("=" * 50)

    if not results:
        print("❗ No matches found in any file. Nothing to export.")
        return None

    # Export results (convert Polars to pandas for Excel)
    print(f"💾 Exporting results to Excel...")
    try:
        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            for value, df_list in results.items():
                # Combine all Polars DataFrames
                if df_list:
                    combined_pl = pl.concat(df_list)
                    combined_pd = combined_pl.to_pandas()
                    
                    sheet_name = str(value)[:31].replace('[', '').replace(']', '').replace('*', '')
                    combined_pd.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"📑 Sheet '{sheet_name}': {len(combined_pd)} rows")

        print(f"📁 Results exported to: {output_excel}")
        
        # Final statistics
        total_rows = sum(len(df_list) for df_list in results.values())
        print(f"🎯 Total matches found: {total_rows} rows across {len(results)} search values")
        
    except Exception as e:
        print(f"❌ Error exporting to Excel: {e}")
        return None
    
    return results