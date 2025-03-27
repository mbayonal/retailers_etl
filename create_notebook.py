#!/usr/bin/env python
# coding: utf-8

import json
import os

# Define the notebook structure
notebook = {
    "cells": [
        # Cell 1: Imports
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "# Standard libraries\n",
                "import os\n",
                "import re\n",
                "import json\n",
                "import logging\n",
                "from datetime import datetime, timedelta\n",
                "from typing import Dict, List, Optional, Tuple, Union, Any\n",
                "from pathlib import Path\n",
                "from dataclasses import dataclass, field\n",
                "\n",
                "# Data processing\n",
                "import pandas as pd\n",
                "import numpy as np\n",
                "\n",
                "# PySpark\n",
                "from pyspark.sql import SparkSession, DataFrame as SparkDataFrame\n",
                "from pyspark.sql import functions as F\n",
                "from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, TimestampType\n",
                "\n",
                "# Great Expectations\n",
                "import great_expectations as ge\n",
                "from great_expectations.dataset import SparkDFDataset\n",
                "\n",
                "# Configure logging\n",
                "logging.basicConfig(\n",
                "    level=logging.INFO,\n",
                "    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'\n",
                ")\n",
                "logger = logging.getLogger('etl_pipeline')"
            ]
        },
        
        # Cell 2: Parameters and Initialization
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "source": [
                "# Configuration Parameters\n",
                "INPUT_DIR = \"/home/manuel-bayona/Documents/crisil2/data\"\n",
                "OUTPUT_DIR = \"/home/manuel-bayona/Documents/crisil2/output\"\n",
                "EXPECTATIONS_PATH = \"/home/manuel-bayona/Documents/crisil2/critical_feature_expectations.json\"\n",
                "\n",
                "# Create Spark session\n",
                "def create_spark_session(app_name: str = \"RetailerETLPipeline\") -> SparkSession:\n",
                "    \"\"\"\n",
                "    Create and configure a Spark session.\n",
                "    \n",
                "    Args:\n",
                "        app_name: Name of the Spark application\n",
                "        \n",
                "    Returns:\n",
                "        Configured SparkSession\n",
                "    \"\"\"\n",
                "    return SparkSession.builder \\\n",
                "        .appName(app_name) \\\n",
                "        .config(\"spark.sql.legacy.timeParserPolicy\", \"LEGACY\") \\\n",
                "        .config(\"spark.sql.legacy.parquet.datetimeRebaseModeInWrite\", \"LEGACY\") \\\n",
                "        .config(\"spark.sql.legacy.parquet.datetimeRebaseModeInRead\", \"LEGACY\") \\\n",
                "        .config(\"spark.sql.adaptive.enabled\", \"true\") \\\n",
                "        .config(\"spark.driver.memory\", \"4g\") \\\n",
                "        .getOrCreate()\n",
                "\n",
                "# Initialize Spark\n",
                "spark = create_spark_session()"
            ]
        }
    ],
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "codemirror_mode": {
                "name": "ipython",
                "version": 3
            },
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.8.10"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

# Read the ETL pipeline code
with open('/home/manuel-bayona/Documents/crisil2/etl_pipeline.py', 'r') as f:
    etl_code = f.read()

# Extract the class definitions and remove the main execution part
lines = etl_code.split('\n')
etl_classes = []
in_class_def = False
for line in lines:
    if line.startswith('class ') or in_class_def:
        in_class_def = True
        etl_classes.append(line)
    if line.startswith('# Example usage'):
        break

# Create the ETL cell content
etl_cell_content = '\n'.join(etl_classes) + '\n\n'
etl_cell_content += """
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
"""

# Add the ETL cell to the notebook
notebook["cells"].append({
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "source": etl_cell_content.split('\n')
})

# Write the notebook to a file
with open('/home/manuel-bayona/Documents/crisil2/etl_pipeline.ipynb', 'w') as f:
    json.dump(notebook, f, indent=2)

print("Notebook created successfully!")
