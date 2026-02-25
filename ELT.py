import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import json

load_dotenv()

# Load credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")

client = bigquery.Client(project=PROJECT_ID)

# -------------------------------------------------------------------------
# User Inputs & Preprocessing
# -------------------------------------------------------------------------
study_id = 1 

# Get campaign ids for the current study from the study_campaigns BQ table
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

study_campaigns_results = list(study_campaigns_job.result())
if not study_campaigns_results:
    raise ValueError(f"No campaigns found for study_id {study_id}")

campaign_ids = study_campaigns_results[0]["campaign_id_array"] 
print(f"Campaign IDs for study {study_id}: {campaign_ids}")

# -------------------------------------------------------------------------
# Data Processing & Transformation Logic
# -------------------------------------------------------------------------

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
        ) as first_imp_ranking
    FROM `{PROJECT_ID}.{DATASET_ID}.survey_responses`
    WHERE measurement_study_id = @study_id
),
FirstResponses AS (
    SELECT * FROM RankedResponses WHERE first_imp_ranking = 1
),
ImpressionStats AS (
    SELECT 
        mock_ip_address,
        MIN(PARSE_DATETIME('%m/%d/%Y %H:%M:%S', date_time)) AS first_imp_dt,
        SUM(ad_cost) AS ad_cost,
        SUM(CAST(is_imp AS INT64)) AS impressions
    FROM `{PROJECT_ID}.{DATASET_ID}.impression_logs`
    WHERE is_imp = TRUE 
      AND ad_campaign_id IN UNNEST(@campaign_ids)  -- Optimized for Arrays
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
    CURRENT_DATETIME() AS updated_at
FROM FirstResponses r
LEFT JOIN ImpressionStats i 
    ON r.mock_ip_address = i.mock_ip_address
WHERE 
    r.response_dt >= i.first_imp_dt 
    OR i.first_imp_dt IS NULL
"""

# -------------------------------------------------------------------------
# Execution & Loading
# -------------------------------------------------------------------------
table_id = f"{PROJECT_ID}.{DATASET_ID}.lift_results"

job_config = bigquery.QueryJobConfig(
    destination=table_id,
    write_disposition="WRITE_TRUNCATE",
    query_parameters=[
        bigquery.ScalarQueryParameter("study_id", "INT64", study_id),
        bigquery.ArrayQueryParameter("campaign_ids", "INT64", campaign_ids)
    ]
)

print("Executing ELT transformation...")
elt_query_job = client.query(sql_elt_q, job_config=job_config)
elt_query_job.result() 

print(f"Success! Updated {table_id}.")