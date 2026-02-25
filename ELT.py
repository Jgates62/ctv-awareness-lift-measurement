"""
ELT: Brand Lift Study Foundational Dataset
Author: Jordan Gates

Purpose:
This script executes a warehouse-native ELT (Extract-Load-Transform) pipeline to 
generate the 'lift_results' table. This table serves as the foundational dataset 
for measuring Brand Lift metrics by joining survey responses with ad exposure logs.

Architectural Decisions:
- ELT: Transformation is pushed to BigQuery to leverage warehouse 
  scalability and minimize egress costs/local memory bottlenecks.
- Parameterized Queries: Used to ensure security and improve query plan caching.
- Attribution Logic: Implements a temporal join to ensure survey responses are 
  only attributed to 'Exposed' groups if the impression occurred BEFORE the survey.
"""

import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment configuration
load_dotenv()

# Load credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")

client = bigquery.Client(project=PROJECT_ID)

# -------------------------------------------------------------------------
# 1. Configuration & Metadata Retrieval
# -------------------------------------------------------------------------
# Target study for analysis. In a production workflow, this could be passed via an orchestrator.
study_id = 3

# Get campaign ids for the current study from the study_campaigns BQ table, 
# allowing us to isolate specific ad spend related to the brand study.
study_campaigns_q = f"""
    SELECT campaign_id_array
    FROM `{PROJECT_ID}.{DATASET_ID}.study_campaigns`
    WHERE study_id = @study_id
    LIMIT 1
"""

study_campaigns_job = client.query(
    study_campaigns_q,
    job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("study_id", "INT64", study_id)]
    )
)

# Handle potential empty results
study_campaigns_results = list(study_campaigns_job.result())
if not study_campaigns_results:
    raise ValueError(f"Data Integrity Error: No campaigns found for study_id {study_id}")

campaign_ids = study_campaigns_results[0]["campaign_id_array"] 
print(f"Processing Study {study_id} for Campaigns: {campaign_ids}")

# -------------------------------------------------------------------------
# 2. Data Processing & Transformation Logic
# -------------------------------------------------------------------------

# Common Table Expressions (CTEs) used here to maintain readability and modularity.
# RankedResponses: Deduplicates responses by taking the earliest timestamp per IP.
# FirstResponses: Selects only the first (earliest) response per IP.
# ImpressionStats: Aggregates ad exposure data at the IP level, calculates 'first_imp_dt' for exposure timing.
# Final Join Logic: LEFT JOIN preserves all survey respondents. The join condition 'r.response_dt >= i.first_imp_dt' ensures correct exposed/control categorization.
# Labeling logic: IF(i.mock_ip_address IS NOT NULL, TRUE, FALSE) labels exposed/control for downstream BI.
sql_elt_q = f"""
WITH RankedResponses AS (
    SELECT
        measurement_study_id,
        mock_ip_address,
        PARSE_DATETIME('%m/%d/%Y %H:%M:%S', date_time) AS response_dt,
        response_value,
        response_name,
        positive_response,
        question,
        ROW_NUMBER() OVER(
            PARTITION BY mock_ip_address 
            ORDER BY PARSE_DATETIME('%m/%d/%Y %H:%M:%S', date_time) ASC
        ) as entry_ranking
    FROM `{PROJECT_ID}.{DATASET_ID}.survey_responses`
    WHERE measurement_study_id = @study_id
),
FirstResponses AS (
    SELECT * FROM RankedResponses WHERE entry_ranking = 1
),
ImpressionStats AS (
    SELECT 
        mock_ip_address,
        MIN(PARSE_DATETIME('%m/%d/%Y %H:%M:%S', date_time)) AS first_imp_dt,
        SUM(ad_cost) AS ad_cost,
        SUM(CAST(is_imp AS INT64)) AS impressions
    FROM `{PROJECT_ID}.{DATASET_ID}.impression_logs`
    WHERE is_imp = TRUE 
      AND ad_campaign_id IN UNNEST(@campaign_ids)
    GROUP BY mock_ip_address
)
SELECT 
    r.measurement_study_id,
    r.mock_ip_address,
    r.response_dt,
    r.response_value,
    r.response_name,
    r.positive_response,
    r.question,
    i.first_imp_dt,
    i.ad_cost,
    i.impressions,
    IF(i.mock_ip_address IS NOT NULL, TRUE, FALSE) AS exposed,
    CURRENT_DATETIME() AS processed_at
FROM FirstResponses r
LEFT JOIN ImpressionStats i 
    ON r.mock_ip_address = i.mock_ip_address
    AND r.response_dt >= i.first_imp_dt
"""

# -------------------------------------------------------------------------
# 3. Execution & Loading
# -------------------------------------------------------------------------
table_id = f"{PROJECT_ID}.{DATASET_ID}.lift_results"

job_config = bigquery.QueryJobConfig(
    destination=table_id,
    write_disposition="WRITE_APPEND", # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    query_parameters=[
        bigquery.ScalarQueryParameter("study_id", "INT64", study_id),
        bigquery.ArrayQueryParameter("campaign_ids", "INT64", campaign_ids)
    ]
)

print(f"Executing ELT transformation and updating {table_id}...")
try:
    elt_query_job = client.query(sql_elt_q, job_config=job_config)
    elt_query_job.result() # Wait for completion
    print(f"Success! Transformation complete.")
except Exception as e:
    print(f"Pipeline Failure: {e}")
    raise

# Optional for local testing: Pull a sample of the results and write to CSV for quick inspection.
n_rows = 500
print(f"\nPreviewing {n_rows} rows from the newly updated table:")
preview_df = client.query(f"SELECT * FROM `{table_id}` LIMIT {n_rows}").to_dataframe()
print(preview_df.head())
preview_df.to_csv("lift_results_preview.csv", index=False)