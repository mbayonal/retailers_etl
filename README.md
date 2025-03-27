# Data Science and Engineering Environment

This project provides a Jupyter notebook environment for data science and engineering tasks using PySpark, Pandas, Polars, Scikit-learn, NumPy, and other libraries.

## Setup

The project uses a Python virtual environment to manage dependencies.

### Virtual Environment

The virtual environment is already set up in the `venv` directory. To activate it:

```bash
source venv/bin/activate
```

To deactivate when you're done:

```bash
deactivate
```

### Dependencies

All required dependencies are listed in `requirements.txt` and have been installed in the virtual environment. The main libraries include:

- Jupyter
- Pandas
- NumPy
- Scikit-learn
- Matplotlib
- Seaborn
- PySpark
- Polars

## Jupyter Notebook

The main notebook file is `eda_analysis.ipynb`, which contains:

1. Setup and imports for all necessary libraries
2. PySpark session initialization
3. Examples of data loading with Pandas, Polars, and PySpark
4. Basic exploratory data analysis techniques
5. Data visualization examples
6. Machine learning examples using both Scikit-learn and PySpark ML

## ETL Pipeline

This project includes a robust ETL (Extract, Transform, Load) pipeline for processing retailer data files. The pipeline is implemented in both Python script format (`etl.py`) and as a Jupyter notebook (`etl_pipeline.ipynb`).

### ETL Architecture

The ETL pipeline follows a modular architecture with the following components:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │     │                 │
│  Data Sources   │────▶│    Extract      │────▶│   Transform     │────▶│     Load        │
│  (Retailers)    │     │  (File Scanner) │     │ (Normalization) │     │ (Output Table)  │
│                 │     │                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                        │                        │
                              │                        │                        │
                              ▼                        ▼                        ▼
                        ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
                        │                 │     │                 │     │                 │
                        │ Schema Manager  │     │ Quality Checker │     │ Metadata Tracker│
                        │                 │     │                 │     │                 │
                        └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### ETL Process Flow

1. **Extract**: The `RetailerFileScanner` component scans the input directory for retailer data files matching specific patterns. It identifies files from different retailers (cubesmart, smartstopselfstorage, storagemart) and extracts metadata from filenames.

2. **Schema Validation**: The `SchemaManager` component validates that the extracted data conforms to the expected schema, ensuring all required columns are present.

3. **Quality Checking**: The `QualityChecker` component performs data quality checks using Great Expectations, applying rules defined in a JSON configuration file.

4. **Transform**: The ETL processor normalizes data types to match the canonical schema and performs any necessary transformations.

5. **Load**: Processed data is written to a unified output table in Parquet format.

6. **Metadata Tracking**: The `MetadataTracker` component logs information about newly detected columns and processing statistics for monitoring and analysis.

### Key Components

#### FileMetadata

A dataclass that stores metadata about retailer data files, including:
- File path
- Retailer name
- Date
- Timestamp

#### RetailerFileScanner

Scans directories for retailer data files matching specific patterns and extracts metadata from filenames.

#### SchemaManager

Manages schema definitions and validation for retailer data, ensuring consistency and providing the canonical schema definition for the unified output.

#### QualityChecker

Performs quality checks on retailer data using Great Expectations, applying data quality rules defined in a JSON configuration file.

#### MetadataTracker

Tracks metadata about processed files and schema changes, logging information about newly detected columns and processing statistics.

#### ETLProcessor

The main orchestrator that coordinates the entire pipeline, handling file scanning, validation, quality checks, transformation, and output writing.

### Running the ETL Pipeline

To run the ETL pipeline:

```bash
# Using the Python script
python etl.py

# Or using the Jupyter notebook
jupyter notebook etl_pipeline.ipynb
```

The ETL pipeline will:
1. Scan for the latest retailer data files
2. Process each file through validation and quality checks
3. Transform the data to match the canonical schema
4. Write the processed data to the unified output table
5. Log metadata about the processing

### Data Flow Diagram

```
┌───────────────┐
│  Input Files  │
│  (Parquet)    │
└───────┬───────┘
        │
        ▼
┌───────────────┐    ┌───────────────┐
│ File Scanner  │───▶│ Schema        │
└───────┬───────┘    │ Validation    │
        │            └───────┬───────┘
        ▼                    │
┌───────────────┐            │
│ Data Loading  │◀───────────┘
└───────┬───────┘
        │
        ▼
┌───────────────┐    ┌───────────────┐
│ Quality       │───▶│ Data          │
│ Checking      │    │ Normalization │
└───────┬───────┘    └───────┬───────┘
        │                    │
        ▼                    ▼
┌───────────────┐    ┌───────────────┐
│ Metadata      │◀───┤ Output        │
│ Tracking      │    │ Writing       │
└───────────────┘    └───────────────┘
```

### Output

The ETL pipeline produces:
- A unified retailer data table in Parquet format
- Metadata logs tracking column changes and processing statistics
- Validation results for data quality checks

## Running the Notebook

To run the notebook, make sure the virtual environment is activated, then:

```bash
jupyter notebook eda_analysis.ipynb
```

Or open it directly in VS Code using the Jupyter extension.

## Additional Information

- The notebook is set up with a sample dataset for demonstration purposes
- Replace the sample data with your actual data files when needed
- Adjust the PySpark configuration settings based on your system resources
