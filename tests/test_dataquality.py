import sys
from pathlib import Path
import pandas as pd


# Add the src directory to the Python path
project_root = Path(__file__).resolve().parent.parent
src_dir = project_root / 'src'
sys.path.append(str(src_dir))

from dbmgr import DBManager
from analytics import data_quality

# Paths to your data files using relative paths
DATASETS_DIR = project_root / "datasets" / "bts_site_b_train"
SCHEMA_PATH = DATASETS_DIR / "Brick_v1.2.1.ttl"
DATA_ZIP_PATH = DATASETS_DIR / "train.zip"
MAPPER_PATH = DATASETS_DIR / "mapper_TrainOnly.csv"
MODEL_PATH = DATASETS_DIR / "Site_B.ttl"

def test_data_quality():
    # Check if files exist
    for path in [SCHEMA_PATH, DATA_ZIP_PATH, MAPPER_PATH, MODEL_PATH]:
        if not path.exists():
            print(f"Error: File not found: {path}")
            return

    print("Initializing DBManager...")
    try:
        db = DBManager(str(DATA_ZIP_PATH), str(MAPPER_PATH), str(MODEL_PATH), str(SCHEMA_PATH))
        print(f"DBManager initialized successfully. Number of streams: {len(db)}")
        print(f"Mapper shape: {db.mapper.shape}")
        print(f"First few rows of mapper:\n{db.mapper.head()}")
        print(f"Data keys: {list(db.data.keys())[:5]}")  # Print first 5 keys if any
    except Exception as e:
        print(f"Error initializing DBManager: {e}")
        return

    print("\nRunning data quality analysis...")
    try:
        results = data_quality.run(db)
        print("Analysis completed successfully.")

        print("\nData Quality Table:")
        print(results["DataQualityTable"])
        
        print("\nSummary Table:")
        print(results["SummaryTable"])

        # Basic assertions to check the structure of the results
        assert "DataQualityTable" in results, "Data quality table is missing from results"
        assert "SummaryTable" in results, "Summary table is missing from results"
        
        # Check if the data quality table has the expected structure
        assert results["DataQualityTable"]["type"] == "table", "Data quality result should be a table"
        assert "header" in results["DataQualityTable"], "Data quality table should have a header"
        assert "cells" in results["DataQualityTable"], "Data quality table should have cells"

        # Similar checks for the summary table
        assert results["SummaryTable"]["type"] == "table", "Summary result should be a table"
        assert "header" in results["SummaryTable"], "Summary table should have a header"
        assert "cells" in results["SummaryTable"], "Summary table should have cells"

        # Check if CSV files were created
        output_dir = Path.cwd() / "output"
        assert (output_dir / "data_quality_table.csv").exists(), "Data quality CSV file not created"
        assert (output_dir / "summary_table.csv").exists(), "Summary table CSV file not created"

        print("\nAll tests passed successfully!")
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_data_quality()
