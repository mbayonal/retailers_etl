#!/usr/bin/env python
# coding: utf-8

"""
ETL Pipeline for Self-Storage Retailer Data

This module implements a modular and reusable ETL pipeline for processing 
self-storage retailer data. It integrates file scanning, validation, QA checks, 
ETL processing, and metadata tracking.

Supported Retailers:
- CubeSmart (`cubesmart_weekly_YYYYMMDD_HHMMSS.parquet`)
- Smart Stop Self Storage (`smartstopselfstorage_weekly_YYYYMMDD_HHMMSS.parquet`)
- Storage Mart (`storagemart_weekly_YYYYMMDD_HHMMSS.parquet`)
"""

# Standard libraries
import os
import re
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
from pathlib import Path
from dataclasses import dataclass, field

# Data processing
import pandas as pd
import numpy as np

# PySpark
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType, TimestampType

# Great Expectations
import great_expectations as ge
from great_expectations.dataset import SparkDFDataset

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('etl_pipeline')


def create_spark_session(app_name: str = "RetailerETLPipeline") -> SparkSession:
    """
    Create and configure a Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()


@dataclass
class FileMetadata:
    """Class to store metadata about a retailer data file."""
    file_path: str
    retailer: str
    date: datetime
    timestamp: str
    
    @property
    def filename(self) -> str:
        """Return just the filename without the path."""
        return os.path.basename(self.file_path)


class RetailerFileScanner:
    """
    Scans directories for retailer data files matching specific patterns.
    
    This class handles the identification of retailer-specific files based on
    naming conventions and extracts metadata from filenames.
    """
    
    # Regex pattern for retailer files
    FILE_PATTERN = r"^(cubesmart|smartstopselfstorage|storagemart)_weekly_(\d{8})_(\d{6})\.parquet$"
    
    def __init__(self, input_dir: str):
        """
        Initialize the file scanner.
        
        Args:
            input_dir: Directory containing retailer data files
        """
        self.input_dir = input_dir
        self.logger = logging.getLogger(f"{__name__}.RetailerFileScanner")
    
    def scan_files(self) -> Dict[str, List[FileMetadata]]:
        """
        Scan the input directory for retailer files.
        
        Returns:
            Dictionary mapping retailer names to lists of file metadata
        """
        retailer_files: Dict[str, List[FileMetadata]] = {}
        
        try:
            for filename in os.listdir(self.input_dir):
                match = re.match(self.FILE_PATTERN, filename)
                if match:
                    retailer, date_str, time_str = match.groups()
                    
                    # Parse date and time
                    file_date = datetime.strptime(date_str, "%Y%m%d")
                    
                    # Create file metadata
                    file_path = os.path.join(self.input_dir, filename)
                    metadata = FileMetadata(
                        file_path=file_path,
                        retailer=retailer,
                        date=file_date,
                        timestamp=time_str
                    )
                    
                    # Add to retailer files dictionary
                    if retailer not in retailer_files:
                        retailer_files[retailer] = []
                    retailer_files[retailer].append(metadata)
            
            # Sort files by date (newest first) for each retailer
            for retailer in retailer_files:
                retailer_files[retailer].sort(key=lambda x: x.date, reverse=True)
            
            self.logger.info(f"Found files for {len(retailer_files)} retailers")
            for retailer, files in retailer_files.items():
                self.logger.info(f"  - {retailer}: {len(files)} files")
            
            return retailer_files
        
        except Exception as e:
            self.logger.error(f"Error scanning files: {str(e)}")
            raise
    
    def get_latest_files(self, reference_date: Optional[datetime] = None) -> Dict[str, FileMetadata]:
        """
        Get the latest file for each retailer.
        
        Args:
            reference_date: Reference date to find the latest file before this date.
                           If None, uses current date minus one day.
        
        Returns:
            Dictionary mapping retailer names to their latest file metadata
        """
        if reference_date is None:
            reference_date = datetime.now() - timedelta(days=1)
        
        retailer_files = self.scan_files()
        latest_files = {}
        
        for retailer, files in retailer_files.items():
            # Filter files before reference date
            valid_files = [f for f in files if f.date <= reference_date]
            
            if valid_files:
                # Get the most recent file
                latest_files[retailer] = valid_files[0]
                self.logger.info(f"Latest file for {retailer}: {latest_files[retailer].filename}")
            else:
                self.logger.warning(f"No valid files found for {retailer} before {reference_date}")
        
        return latest_files


class SchemaManager:
    """
    Manages schema definitions and validation for retailer data.
    
    This class handles schema consistency checks and provides the canonical
    schema definition for the unified output.
    """
    
    def __init__(self):
        """Initialize the schema manager with canonical schema definitions."""
        self.logger = logging.getLogger(f"{__name__}.SchemaManager")
        
        # Define canonical schema for PySpark
        self.canonical_schema = StructType([
            StructField("Run_Date", TimestampType(), False),
            StructField("Retailer", StringType(), False),
            StructField("Retailer_Website", StringType(), True),
            StructField("Store_Info", StringType(), False),
            StructField("Category_Traversal", StringType(), False),
            StructField("Brand", StringType(), False),
            StructField("Product_Name", StringType(), False),
            StructField("Product_ID", StringType(), False),
            StructField("Universal_Product_ID", StringType(), True),
            StructField("Variant_Info", StringType(), False),
            StructField("Size_Or_Quantity", StringType(), False),
            StructField("Number_Of_Reviews", FloatType(), True),
            StructField("Average_Rating", FloatType(), True),
            StructField("Original_Price", FloatType(), True),
            StructField("Sale_Price", FloatType(), True),
            StructField("Currency", StringType(), True),
            StructField("Url", StringType(), True),
            StructField("In_Stock", BooleanType(), False)
        ])
        
        # List of required columns that must be present
        self.required_columns = [
            "Run_Date", "Retailer", "Store_Info", "Category_Traversal",
            "Brand", "Product_Name", "Product_ID", "Variant_Info",
            "Size_Or_Quantity", "In_Stock"
        ]
    
    def validate_schema(self, df: SparkDataFrame) -> Tuple[bool, List[str]]:
        """
        Validate that a dataframe has all required columns.
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Tuple of (is_valid, missing_columns)
        """
        df_columns = set(df.columns)
        required_columns = set(self.required_columns)
        
        missing_columns = required_columns - df_columns
        
        is_valid = len(missing_columns) == 0
        
        if not is_valid:
            self.logger.warning(f"Schema validation failed. Missing columns: {missing_columns}")
        else:
            self.logger.info("Schema validation passed")
        
        return is_valid, list(missing_columns)
    
    def detect_new_columns(self, df: SparkDataFrame) -> List[str]:
        """
        Detect columns in the dataframe that are not in the canonical schema.
        
        Args:
            df: Spark DataFrame to check
            
        Returns:
            List of new column names
        """
        canonical_columns = {field.name for field in self.canonical_schema.fields}
        df_columns = set(df.columns)
        
        new_columns = df_columns - canonical_columns
        
        if new_columns:
            self.logger.info(f"Detected {len(new_columns)} new columns: {new_columns}")
        
        return list(new_columns)
    
    def get_canonical_columns(self) -> List[str]:
        """
        Get the list of canonical column names.
        
        Returns:
            List of column names in the canonical schema
        """
        return [field.name for field in self.canonical_schema.fields]


class QualityChecker:
    """
    Performs quality checks on retailer data using Great Expectations.
    
    This class applies data quality rules defined in a JSON configuration file
    and reports on validation results.
    """
    
    def __init__(self, expectations_path: str):
        """
        Initialize the quality checker.
        
        Args:
            expectations_path: Path to the JSON file containing expectations
        """
        self.logger = logging.getLogger(f"{__name__}.QualityChecker")
        self.expectations_path = expectations_path
        self.expectations = self._load_expectations()
    
    def _load_expectations(self) -> List[Dict[str, Any]]:
        """
        Load expectations from the JSON configuration file.
        
        Returns:
            List of expectation configurations
        """
        try:
            with open(self.expectations_path, 'r') as f:
                config = json.load(f)
                return config.get('expectations', [])
        except Exception as e:
            self.logger.error(f"Failed to load expectations: {str(e)}")
            raise
    
    def validate_dataframe(self, df: SparkDataFrame) -> Tuple[bool, Dict[str, Any], List[int]]:
        try:
            pandas_df = df.toPandas()
            ge_df = ge.from_pandas(pandas_df)
            
            validation_results = {"success": True, "results": []}
            failing_indices = set()

            for expectation in self.expectations:
                expectation_type = expectation.get('expectation_type')
                kwargs = expectation.get('kwargs', {})
                column = kwargs.get('column')

                if column and column not in df.columns:
                    self.logger.warning(f"Column '{column}' not found, skipping expectation: {expectation_type}")
                    continue

                try:
                    if expectation_type == "ExpectColumnValuesToNotBeNull":
                        result = ge_df.expect_column_values_to_not_be_null(**kwargs)
                        if not result["success"]:
                            failed_rows = pandas_df[pandas_df[column].isnull()]
                            failing_indices.update(failed_rows.index.tolist())

                    elif expectation_type == "ExpectColumnValuesToBeUnique":
                        duplicates = pandas_df[pandas_df.duplicated(column, keep=False)]
                        result = ge_df.expect_column_values_to_be_unique(**kwargs)
                        if not result["success"]:
                            failing_indices.update(duplicates.index.tolist())

                    elif expectation_type == "ExpectColumnValuesToBeBetween":
                        min_val = kwargs.get("min_value", float('-inf'))
                        max_val = kwargs.get("max_value", float('inf'))
                        result = ge_df.expect_column_values_to_be_between(**kwargs)
                        if not result["success"]:
                            failed_rows = pandas_df[(pandas_df[column] < min_val) | (pandas_df[column] > max_val)]
                            failing_indices.update(failed_rows.index.tolist())

                    else:
                        self.logger.warning(f"Unsupported expectation type: {expectation_type}")
                        continue

                    validation_results["results"].append(result)

                    if not result["success"]:
                        validation_results["success"] = False
                        self.logger.warning(f"Failed expectation: {expectation_type} for {kwargs}")

                except Exception as e:
                    self.logger.error(f"Error applying expectation {expectation_type}: {str(e)}")
                    validation_results["success"] = False
                    validation_results["results"].append({
                        "expectation_type": expectation_type,
                        "kwargs": kwargs,
                        "success": False,
                        "error": str(e)
                    })

            return validation_results["success"], validation_results, list(failing_indices)

        except Exception as e:
            self.logger.error(f"Error during validation: {str(e)}")
            return False, {"success": False, "error": str(e)}, []



class MetadataTracker:
    """
    Tracks metadata about processed files and schema changes.
    
    This class logs information about newly detected columns and processing
    statistics to enable monitoring and analysis.
    """
    
    def __init__(self, output_dir: str):
        """
        Initialize the metadata tracker.
        
        Args:
            output_dir: Directory to store metadata logs
        """
        self.logger = logging.getLogger(f"{__name__}.MetadataTracker")
        self.output_dir = output_dir
        self.metadata_file = os.path.join(output_dir, "column_metadata.parquet")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def log_new_columns(self, file_name: str, new_columns: List[str], spark: SparkSession) -> None:
        """
        Log newly detected columns to the metadata table.
        
        Args:
            file_name: Name of the source file
            new_columns: List of new column names
            spark: SparkSession to use for writing
        """
        if not new_columns:
            self.logger.info(f"No new columns detected in {file_name}")
            return
        
        # Create a dataframe with the new column information
        data = [(file_name, col, datetime.now()) for col in new_columns]
        schema = StructType([
            StructField("file_name", StringType(), False),
            StructField("new_column", StringType(), False),
            StructField("detection_time", TimestampType(), False)
        ])
        
        new_columns_df = spark.createDataFrame(data, schema)
        
        # Append to existing metadata file if it exists, otherwise create it
        if os.path.exists(self.metadata_file):
            existing_df = spark.read.parquet(self.metadata_file)
            combined_df = existing_df.union(new_columns_df)
            combined_df.write.mode("overwrite").parquet(self.metadata_file)
        else:
            new_columns_df.write.mode("overwrite").parquet(self.metadata_file)
        
        self.logger.info(f"Logged {len(new_columns)} new columns for {file_name}")
    
    def get_column_metadata(self, spark: SparkSession) -> Optional[SparkDataFrame]:
        """
        Retrieve the column metadata table.
        
        Args:
            spark: SparkSession to use for reading
            
        Returns:
            Spark DataFrame containing column metadata, or None if not found
        """
        if os.path.exists(self.metadata_file):
            return spark.read.parquet(self.metadata_file)
        return None


class ETLProcessor:
    """
    Main ETL processor that orchestrates the entire pipeline.
    
    This class coordinates file scanning, validation, quality checks,
    transformation, and output writing.
    """
    
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        expectations_path: str,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the ETL processor.
        
        Args:
            input_dir: Directory containing input files
            output_dir: Directory for output files and metadata
            expectations_path: Path to expectations JSON file
            spark: Optional SparkSession (will create one if not provided)
        """
        self.logger = logging.getLogger(f"{__name__}.ETLProcessor")
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.expectations_path = expectations_path
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize Spark session if not provided
        self.spark = spark if spark else create_spark_session()
        
        # Initialize components
        self.file_scanner = RetailerFileScanner(input_dir)
        self.schema_manager = SchemaManager()
        self.quality_checker = QualityChecker(expectations_path)
        self.metadata_tracker = MetadataTracker(output_dir)
        
        # Output table path
        self.output_table_path = os.path.join(output_dir, "unified_retailer_data.parquet")

    def _cast_pandas_to_canonical_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cast pandas DataFrame columns to match the canonical Spark schema.
        Handles Spark-to-Pandas type translation for safe conversion.
        """
        for field in self.schema_manager.canonical_schema.fields:
            col_name = field.name
            if col_name not in df.columns:
                continue

            try:
                spark_type = field.dataType

                if isinstance(spark_type, FloatType):
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype(float)

                elif isinstance(spark_type, IntegerType):
                    # Use nullable pandas Int64 to support missing values
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')

                elif isinstance(spark_type, BooleanType):
                    df[col_name] = df[col_name].astype(bool)

                elif isinstance(spark_type, TimestampType):
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')

                elif isinstance(spark_type, StringType):
                    # Explicitly cast to string to handle mixed object types
                    df[col_name] = df[col_name].astype(str)

                else:
                    self.logger.warning(f"Unhandled Spark type for column {col_name}: {type(spark_type)}")

            except Exception as e:
                self.logger.warning(f"Failed to cast column {col_name}: {str(e)}")

        return df
  
    def process_file(self, file_metadata: FileMetadata) -> Optional[SparkDataFrame]:
        """
        Process a single retailer file through the ETL pipeline.
        
        Args:
            file_metadata: Metadata for the file to process
            
        Returns:
            Processed Spark DataFrame, or None if processing failed
        """
        file_path = file_metadata.file_path
        retailer = file_metadata.retailer
        
        try:
            # Read the file
            self.logger.info(f"Processing file: {file_metadata.filename}")
            df = self.spark.read.parquet(file_path)
            
            # Validate schema
            is_valid, missing_columns = self.schema_manager.validate_schema(df)
            if not is_valid:
                self.logger.error(f"Schema validation failed for {file_metadata.filename}. Missing columns: {missing_columns}")
                return None
            
            # Detect new columns for metadata tracking
            new_columns = self.schema_manager.detect_new_columns(df)
            self.metadata_tracker.log_new_columns(file_metadata.filename, new_columns, self.spark)
            
            # Perform quality checks
            passed_qa, qa_results, failing_indices = self.quality_checker.validate_dataframe(df)

            if failing_indices:
                self.logger.warning(f"Filtering out {len(failing_indices)} failing records from {file_metadata.filename}")
                pandas_df = df.toPandas()
                valid_df = pandas_df.drop(index=failing_indices)

                if valid_df.empty:
                    self.logger.warning(f"All records failed quality checks in {file_metadata.filename}")
                    return None

                # NEW: cast pandas columns to canonical types
                valid_df = self._cast_pandas_to_canonical_types(valid_df)

                df = self.spark.createDataFrame(valid_df, schema=self.schema_manager.canonical_schema)

            elif not passed_qa:
                self.logger.error(f"Quality checks failed entirely for {file_metadata.filename}")
                return None
            
            # Normalize data types
            normalized_df = self._normalize_dataframe(df)
            
            # Select only canonical columns
            canonical_columns = self.schema_manager.get_canonical_columns()
            output_df = normalized_df.select(*[col for col in canonical_columns if col in normalized_df.columns])
            
            self.logger.info(f"Successfully processed {file_metadata.filename}")
            return output_df
        
        except Exception as e:
            self.logger.error(f"Error processing file {file_metadata.filename}: {str(e)}")
            return None
    
    def _normalize_dataframe(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Normalize data types in the dataframe to match canonical schema.
        
        Args:
            df: Spark DataFrame to normalize
            
        Returns:
            Normalized Spark DataFrame
        """
        normalized_df = df
        
        # Apply type conversions
        for field in self.schema_manager.canonical_schema.fields:
            if field.name in df.columns:
                col_name = field.name
                
                if isinstance(field.dataType, TimestampType):
                    normalized_df = normalized_df.withColumn(
                        col_name, 
                        F.to_timestamp(F.col(col_name))
                    )
                elif isinstance(field.dataType, FloatType):
                    normalized_df = normalized_df.withColumn(
                        col_name,
                        F.col(col_name).cast("float")
                    )
                elif isinstance(field.dataType, BooleanType):
                    normalized_df = normalized_df.withColumn(
                        col_name,
                        F.col(col_name).cast("boolean")
                    )
                elif isinstance(field.dataType, StringType):
                    normalized_df = normalized_df.withColumn(
                        col_name,
                        F.col(col_name).cast("string")
                    )
        
        return normalized_df
    
    def run_pipeline(self, reference_date: Optional[datetime] = None) -> bool:
        """
        Run the complete ETL pipeline.
        
        Args:
            reference_date: Reference date for finding latest files
            
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        try:
            # Get latest files for each retailer
            latest_files = self.file_scanner.get_latest_files(reference_date)
            
            if not latest_files:
                self.logger.warning("No files found to process")
                return False
            
            # Process each file
            processed_dfs = []
            for retailer, file_metadata in latest_files.items():
                processed_df = self.process_file(file_metadata)
                if processed_df:
                    processed_dfs.append(processed_df)
            
            if not processed_dfs:
                self.logger.error("No files were successfully processed")
                return False
            
            # Combine all processed dataframes
            if len(processed_dfs) > 1:
                combined_df = processed_dfs[0]
                for df in processed_dfs[1:]:
                    combined_df = combined_df.union(df)
            else:
                combined_df = processed_dfs[0]
            
            # Write to output table
            self._write_to_output_table(combined_df)
            
            self.logger.info("ETL pipeline completed successfully")
            return True
        
        except Exception as e:
            self.logger.error(f"Error running ETL pipeline: {str(e)}")
            return False
    
    def _write_to_output_table(self, df: SparkDataFrame) -> None:
        """
        Write processed data to the unified output table.
        
        Args:
            df: Spark DataFrame to write
        """
        # Check if output table exists
        output_exists = os.path.exists(self.output_table_path)
        
        if output_exists:
            # Append to existing table
            df.write.mode("append").parquet(self.output_table_path)
            self.logger.info(f"Appended data to existing output table: {self.output_table_path}")
        else:
            # Create new table
            df.write.mode("overwrite").parquet(self.output_table_path)
            self.logger.info(f"Created new output table: {self.output_table_path}")


# Example usage
if __name__ == "__main__":
    # Configuration
    INPUT_DIR = "/home/manuel-bayona/Documents/crisil2/data"
    OUTPUT_DIR = "/home/manuel-bayona/Documents/crisil2/output"
    EXPECTATIONS_PATH = "/home/manuel-bayona/Documents/crisil2/critical_feature_expectations.json"
    
    # Create Spark session
    spark = create_spark_session()
    
    # Initialize and run ETL pipeline
    etl = ETLProcessor(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        expectations_path=EXPECTATIONS_PATH,
        spark=spark
    )
    
    success = etl.run_pipeline()
    
    if success:
        print("ETL pipeline completed successfully")
    else:
        print("ETL pipeline failed")
