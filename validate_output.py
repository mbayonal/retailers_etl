#!/usr/bin/env python
# coding: utf-8

"""
Validation Script for ETL Pipeline Output

This script validates the output parquet files from the ETL pipeline for:
1. Schema consistency against the canonical schema
2. Data type correctness
3. Duplicate records
4. Null values in required fields

The script does not modify any existing ETL processing scripts and
serves as a standalone validation tool.
"""

import os
import logging
import json
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Set

# Data processing
import pandas as pd
import numpy as np

# PySpark
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('output_validator')


def create_spark_session(app_name: str = "OutputValidator") -> SparkSession:
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


class OutputValidator:
    """
    Validates the ETL pipeline output for data quality and consistency.
    
    This class performs various checks on the unified output parquet files
    to ensure data integrity and consistency with the canonical schema.
    """
    
    def __init__(self, output_dir: str, expectations_path: str, spark: Optional[SparkSession] = None):
        """
        Initialize the output validator.
        
        Args:
            output_dir: Directory containing the output parquet files
            expectations_path: Path to the expectations JSON file
            spark: Optional SparkSession (will create one if not provided)
        """
        self.logger = logging.getLogger(f"{__name__}.OutputValidator")
        self.output_dir = output_dir
        self.expectations_path = expectations_path
        
        # Initialize Spark session if not provided
        self.spark = spark if spark else create_spark_session()
        
        # Output table path
        self.output_table_path = os.path.join(output_dir, "unified_retailer_data.parquet")
        
        # Define canonical schema
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
        
        # Load expectations
        self.expectations = self._load_expectations()
        
        # Validation results
        self.validation_results = {
            "schema_validation": {"passed": False, "details": {}},
            "data_type_validation": {"passed": False, "details": {}},
            "duplicate_check": {"passed": False, "details": {}},
            "null_check": {"passed": False, "details": {}}
        }
    
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
            return []
    
    def load_output_data(self) -> Optional[SparkDataFrame]:
        """
        Load the unified output data from parquet files.
        
        Returns:
            Spark DataFrame containing the output data, or None if not found
        """
        if not os.path.exists(self.output_table_path):
            self.logger.error(f"Output table not found at: {self.output_table_path}")
            return None
        
        try:
            df = self.spark.read.parquet(self.output_table_path)
            self.logger.info(f"Loaded output data with {df.count()} rows and {len(df.columns)} columns")
            return df
        except Exception as e:
            self.logger.error(f"Error loading output data: {str(e)}")
            return None
    
    def validate_schema(self, df: SparkDataFrame) -> bool:
        """
        Validate that the output data has the correct schema structure.
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            True if schema is valid, False otherwise
        """
        # Check for missing required columns
        df_columns = set(df.columns)
        required_columns = set(self.required_columns)
        
        missing_columns = required_columns - df_columns
        
        # Check for extra columns not in canonical schema
        canonical_columns = {field.name for field in self.canonical_schema.fields}
        extra_columns = df_columns - canonical_columns
        
        # Store results
        self.validation_results["schema_validation"]["details"]["missing_columns"] = list(missing_columns)
        self.validation_results["schema_validation"]["details"]["extra_columns"] = list(extra_columns)
        self.validation_results["schema_validation"]["passed"] = len(missing_columns) == 0
        
        if missing_columns:
            self.logger.error(f"Schema validation failed. Missing columns: {missing_columns}")
        else:
            self.logger.info("Schema validation passed")
        
        if extra_columns:
            self.logger.warning(f"Found {len(extra_columns)} columns not in canonical schema: {extra_columns}")
        
        return len(missing_columns) == 0
    
    def validate_data_types(self, df: SparkDataFrame) -> bool:
        """
        Validate that the output data has the correct data types.
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            True if data types are valid, False otherwise
        """
        type_mismatches = []
        
        # Check each field in the canonical schema
        for field in self.canonical_schema.fields:
            col_name = field.name
            if col_name not in df.columns:
                continue
            
            # Get the actual data type from the DataFrame
            actual_type = df.schema[col_name].dataType
            expected_type = field.dataType
            
            # Check if types match (considering type compatibility)
            if not self._are_types_compatible(actual_type, expected_type):
                type_mismatches.append({
                    "column": col_name,
                    "expected_type": str(expected_type),
                    "actual_type": str(actual_type)
                })
        
        # Store results
        self.validation_results["data_type_validation"]["details"]["type_mismatches"] = type_mismatches
        self.validation_results["data_type_validation"]["passed"] = len(type_mismatches) == 0
        
        if type_mismatches:
            self.logger.error(f"Data type validation failed. Found {len(type_mismatches)} type mismatches")
            for mismatch in type_mismatches:
                self.logger.error(f"  - Column '{mismatch['column']}': expected {mismatch['expected_type']}, got {mismatch['actual_type']}")
        else:
            self.logger.info("Data type validation passed")
        
        return len(type_mismatches) == 0
    
    def _are_types_compatible(self, actual_type, expected_type) -> bool:
        """
        Check if two Spark data types are compatible.
        
        Args:
            actual_type: Actual data type
            expected_type: Expected data type
            
        Returns:
            True if types are compatible, False otherwise
        """
        # Convert types to strings for comparison
        actual_str = str(actual_type).lower()
        expected_str = str(expected_type).lower()
        
        # Exact match
        if actual_str == expected_str:
            return True
        
        # Check for numeric type compatibility
        if ('int' in actual_str and 'int' in expected_str) or \
           ('float' in actual_str and 'float' in expected_str) or \
           ('double' in actual_str and 'float' in expected_str) or \
           ('float' in actual_str and 'double' in expected_str):
            return True
        
        # Check for string type compatibility
        if 'string' in actual_str and 'string' in expected_str:
            return True
        
        # Check for timestamp/date compatibility
        if ('timestamp' in actual_str and 'timestamp' in expected_str) or \
           ('date' in actual_str and 'date' in expected_str) or \
           ('timestamp' in actual_str and 'date' in expected_str):
            return True
        
        return False
    
    def check_for_duplicates(self, df: SparkDataFrame) -> bool:
        """
        Check for duplicate records in the output data.
        
        Args:
            df: Spark DataFrame to check
            
        Returns:
            True if no duplicates found, False otherwise
        """
        # Create a composite key for uniqueness check
        # Using retailer_id, store_id, product_id, transaction_date as the composite key
        composite_key_cols = ["Retailer", "Store_Info", "Product_ID", "Run_Date"]
        available_cols = [col for col in composite_key_cols if col in df.columns]
        
        if not all(col in df.columns for col in composite_key_cols):
            missing_cols = [col for col in composite_key_cols if col not in df.columns]
            self.logger.warning(f"Some columns for duplicate check are missing: {missing_cols}")
            if not available_cols:
                self.logger.error("Cannot perform duplicate check: all key columns are missing")
                self.validation_results["duplicate_check"]["details"]["error"] = "Missing key columns for duplicate check"
                self.validation_results["duplicate_check"]["passed"] = False
                return False
        
        # Count records before deduplication
        total_count = df.count()
        
        # Count unique records
        unique_count = df.dropDuplicates(available_cols).count()
        
        # Calculate duplicates
        duplicate_count = total_count - unique_count
        
        # Store results
        self.validation_results["duplicate_check"]["details"]["total_records"] = total_count
        self.validation_results["duplicate_check"]["details"]["unique_records"] = unique_count
        self.validation_results["duplicate_check"]["details"]["duplicate_records"] = duplicate_count
        self.validation_results["duplicate_check"]["passed"] = duplicate_count == 0
        
        if duplicate_count > 0:
            self.logger.error(f"Found {duplicate_count} duplicate records out of {total_count} total records")
            
            # Get sample of duplicates for reporting
            window = F.Window.partitionBy(*available_cols).orderBy(F.lit(1))
            dup_count = F.count("*").over(window)
            
            duplicates = df.withColumn("dup_count", dup_count) \
                           .filter(F.col("dup_count") > 1) \
                           .orderBy(*available_cols) \
                           .limit(10)  # Limit to 10 examples
            
            if duplicates.count() > 0:
                self.logger.error("Sample of duplicate records:")
                sample_duplicates = duplicates.select(*available_cols).distinct().limit(5).collect()
                for row in sample_duplicates:
                    self.logger.error(f"  - {', '.join([f'{col}={row[col]}' for col in available_cols])}")
                
                self.validation_results["duplicate_check"]["details"]["sample_duplicates"] = [
                    {col: row[col] for col in available_cols} for row in sample_duplicates
                ]
        else:
            self.logger.info("No duplicate records found")
        
        return duplicate_count == 0
    
    def check_for_nulls(self, df: SparkDataFrame) -> bool:
        """
        Check for null values in required fields.
        
        Args:
            df: Spark DataFrame to check
            
        Returns:
            True if no nulls found in required fields, False otherwise
        """
        null_counts = {}
        has_nulls = False
        
        # Check each required column for nulls
        for col_name in self.required_columns:
            if col_name not in df.columns:
                continue
            
            # Count nulls in this column
            null_count = df.filter(F.col(col_name).isNull()).count()
            
            if null_count > 0:
                has_nulls = True
                null_counts[col_name] = null_count
        
        # Store results
        self.validation_results["null_check"]["details"]["null_counts"] = null_counts
        self.validation_results["null_check"]["passed"] = not has_nulls
        
        if has_nulls:
            self.logger.error(f"Found null values in required columns: {null_counts}")
        else:
            self.logger.info("No null values found in required columns")
        
        return not has_nulls
    
    def run_validation(self) -> Dict[str, Any]:
        """
        Run all validation checks on the output data.
        
        Returns:
            Dictionary containing validation results
        """
        self.logger.info("Starting validation of ETL output data")
        
        # Load output data
        df = self.load_output_data()
        if df is None:
            self.logger.error("Validation failed: Could not load output data")
            return self.validation_results
        
        # Run validation checks
        schema_valid = self.validate_schema(df)
        types_valid = self.validate_data_types(df)
        no_duplicates = self.check_for_duplicates(df)
        no_nulls = self.check_for_nulls(df)
        
        # Overall validation result
        overall_passed = schema_valid and types_valid and no_duplicates and no_nulls
        
        self.validation_results["overall_passed"] = overall_passed
        self.validation_results["timestamp"] = datetime.now().isoformat()
        
        if overall_passed:
            self.logger.info("All validation checks passed!")
        else:
            self.logger.error("Validation failed. See details for specific issues.")
        
        return self.validation_results
    
    def save_validation_results(self, output_path: str) -> None:
        """
        Save validation results to a JSON file.
        
        Args:
            output_path: Path to save the validation results
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(self.validation_results, f, indent=2)
            self.logger.info(f"Validation results saved to: {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving validation results: {str(e)}")


def main():
    """Main entry point for the validation script."""
    parser = argparse.ArgumentParser(description='Validate ETL pipeline output data')
    parser.add_argument('--output-dir', type=str, default="/home/manuel-bayona/Documents/crisil2/output",
                        help='Directory containing output parquet files')
    parser.add_argument('--expectations-path', type=str, 
                        default="/home/manuel-bayona/Documents/crisil2/critical_feature_expectations.json",
                        help='Path to expectations JSON file')
    parser.add_argument('--results-path', type=str, 
                        default="/home/manuel-bayona/Documents/crisil2/validation_results.json",
                        help='Path to save validation results')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    # Initialize and run validator
    validator = OutputValidator(
        output_dir=args.output_dir,
        expectations_path=args.expectations_path,
        spark=spark
    )
    
    # Run validation
    results = validator.run_validation()
    
    # Save results
    validator.save_validation_results(args.results_path)
    
    # Stop Spark session
    spark.stop()
    
    # Return exit code based on validation result
    return 0 if results.get("overall_passed", False) else 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
