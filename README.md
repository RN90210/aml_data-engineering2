# MuleDetector — Static Data Engineering Pipeline

This repository contains the completed static PySpark data-engineering pipeline for the MuleDetector AML project.

## Scope

The pipeline:

1. Reads the HI-Medium transaction and account CSV files.
2. Joins account metadata for both the sender and receiver.
3. Creates the final Account, Bank, and Entity node features.
4. Creates Account-to-Account transaction edges and metadata relations.
5. Creates offline Account labels using `Is Laundering`.

`Is Laundering` is used only for offline labels. It is never used as a model input feature.

## Run the pipeline

```powershell
# Small validation run
python -m preprocessing.spark_static_pipeline

# Full historical static run
python -m preprocessing.spark_static_pipeline --full