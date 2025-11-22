import polars as pl
from pathlib import Path
from datetime import datetime

def process_demographics_fast(input_folder: str, output_folder: str, num_partitions: int = None, ui_process_data=None) -> None:
    """
    High-performance function to read CSV and Parquet files, process them,
    and save partitioned by specified number of partitions.
    
    Args:
        input_folder: Path to input folder containing CSV and Parquet files
        output_folder: Path to output folder for saving partitioned data
        num_partitions: Number of partitions to split the data (optional)
    """
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Read all Parquet files in parallel
    parquet_files = list(input_path.glob("*.parquet"))
    parquet_df = None
    if parquet_files:
        print(f"Found {len(parquet_files)} Parquet files")
        parquet_df = pl.read_parquet(parquet_files)
        
        # Convert ALL columns to string
        parquet_df = parquet_df.cast(pl.Utf8)
        print("Parquet columns:", parquet_df.columns)
        print("Parquet shape:", parquet_df.shape)
        print("Parquet dtypes:", parquet_df.dtypes)
    else:
        print("No Parquet files found")
    
    # Read all CSV files with optimized settings
    csv_files = list(input_path.glob("*.csv"))
    csv_df = None
    if csv_files:
        print(f"Found {len(csv_files)} CSV files")
        
        # Read CSV files one by one and concatenate
        csv_dfs = []
        for csv_file in csv_files:
            print(f"Reading CSV: {csv_file.name}")
            try:
                df = pl.read_csv(
                    str(csv_file),
                    separator=';',
                    infer_schema_length=10000,
                    low_memory=True,
                    rechunk=True,
                    try_parse_dates=True
                )
                # Convert ALL columns to string
                df = df.cast(pl.Utf8)
                csv_dfs.append(df)
                print(f"CSV {csv_file.name} shape: {df.shape}")
            except Exception as e:
                print(f"Error reading {csv_file}: {e}")
        
        if csv_dfs:
            csv_df = pl.concat(csv_dfs, how="diagonal")
            
            # Convert again to ensure all are string after concat
            csv_df = csv_df.cast(pl.Utf8)
            
            # Rename CSV columns to match Parquet structure
            csv_df = csv_df.rename({
                "identificacion": "document",
                "cuenta": "account", 
                "dato": "demographic"
            })
            
            print("CSV columns after renaming:", csv_df.columns)
            print("CSV shape:", csv_df.shape)
        else:
            print("No CSV files could be read successfully")
    else:
        print("No CSV files found")
    
    # Create unified dataframe
    unified_df = None
    
    if parquet_df is not None and csv_df is not None:
        print("Creating unified dataframe...")
        
        # Select only the common columns from both dataframes
        parquet_selected = parquet_df.select(["document", "account", "demographic"])
        csv_selected = csv_df.select(["document", "account", "demographic"])
        
        # Combine both dataframes
        combined_df = pl.concat([parquet_selected, csv_selected], how="vertical")
        
        # Remove duplicates based on the key (account + demographic)
        unified_df = combined_df.unique(subset=["account", "demographic"])
        
        print("Unified columns:", unified_df.columns)
        print("Unified shape after removing duplicates:", unified_df.shape)
        
    elif parquet_df is not None:
        print("Only Parquet data available")
        unified_df = parquet_df.select(["document", "account", "demographic"]).unique(subset=["account", "demographic"])
        
    elif csv_df is not None:
        print("Only CSV data available") 
        unified_df = csv_df.select(["document", "account", "demographic"]).unique(subset=["account", "demographic"])
    
    if unified_df is not None:
        # Classify type based on demographic
        unified_df = unified_df.with_columns(
            type=pl.when(
                (pl.col("demographic").str.starts_with("6")) & 
                (pl.col("demographic").str.len_chars() == 10)
            ).then(pl.lit("fijo"))
            .when(
                (pl.col("demographic").str.starts_with("3")) & 
                (pl.col("demographic").str.len_chars() == 10)
            ).then(pl.lit("celular"))
            .when(
                pl.col("demographic").str.contains("@")
            ).then(pl.lit("email"))
            .otherwise(pl.lit("error"))
        )
        
        # Count types for reporting
        type_counts = unified_df.group_by("type").agg(pl.count().alias("count"))
        print("Type distribution:")
        print(type_counts)
        
        # Generate filename with current date
        current_date = datetime.now().strftime("%Y-%m-%d")
        base_filename = f"demographics_unify_{current_date}"
        
        # Save unified data with partitions
        if num_partitions and num_partitions > 1:
            print(f"Saving unified data in {num_partitions} partitions...")
            
            # Calculate partition size
            total_rows = unified_df.height
            partition_size = total_rows // num_partitions
            
            # Split and save partitions
            for i in range(num_partitions):
                start_idx = i * partition_size
                end_idx = (i + 1) * partition_size if i < num_partitions - 1 else total_rows
                
                partition_df = unified_df.slice(start_idx, end_idx - start_idx)
                partition_path = output_path / f"{base_filename}_part_{i+1}.parquet"
                partition_df.write_parquet(partition_path, use_pyarrow=False)
                print(f"Saved partition {i+1}: {partition_path}")
        else:
            # Save as single file
            output_file = output_path / f"{base_filename}.parquet"
            unified_df.write_parquet(output_file, use_pyarrow=False)
            print(f"Saved unified data: {output_file}")
        
        print(f"Processing completed successfully!")
    else:
        print("No data available to process")
    
    print("Final processing completed!")