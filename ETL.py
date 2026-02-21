#Setup
import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Load credentials from .env (same as data_generator.py)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

print(f"Connected to project: {PROJECT_ID}, dataset: {DATASET_ID}")

# Helper function to run SQL and return a DataFrame.
def query_bq(sql: str) -> pd.DataFrame:
    """Run a SQL query against BigQuery and return a pandas DataFrame."""
    return client.query(sql).to_dataframe()

# Inputs
study_id = 1
campaign_ids = "103, 104"

# Extract

# Get a df of users who responded to the survey (i.e. those who were in the test group and completed the survey)
sql_responses_q = f"""
SELECT
  measurement_study_id,
  mock_ip_address,
  date_time as response_dt,
  response_value,
  response_name,
  positive_response,
  question
FROM (
  SELECT
    measurement_study_id,
    mock_ip_address,
    date_time,
    response_value,
    response_name,
    positive_response,
    question,
    -- Creates a ranking for each IP, starting at 1 for the earliest date_time
    ROW_NUMBER() OVER (
      PARTITION BY mock_ip_address 
      ORDER BY date_time ASC
    ) as entry_order
  FROM `{PROJECT_ID}.{DATASET_ID}.survey_responses`
  WHERE
    measurement_study_id = {study_id}
)
WHERE
  entry_order = 1
"""

df_responses = query_bq(sql_responses_q)

print(f"Rows: {len(df_responses):,}")
df_responses.head()

response_ips = ", ".join([f"'{ip}'" for ip in df_responses["mock_ip_address"].unique().tolist()])
# print(response_ips)
# Load Impression Logs
# sql_impressions = f"""
# SELECT
#     ad_campaign_id,
#     ad_campaign_name,
#     date_time,
#     mock_ip_address,
#     ad_cost,
#     is_imp,
#     ad_started,
#     ad_completed
# FROM `{PROJECT_ID}.{DATASET_ID}.impression_logs`
# WHERE 
#     ad_campaign_id IN ({campaign_ids})
# """
exposed_ip_q = f"""
SELECT 
  mock_ip_address,
  MIN(date_time) as first_imp_dt,
  sum(ad_cost) as ad_cost,
  sum(CAST(is_imp AS INT64)) as impressions
FROM `{PROJECT_ID}.{DATASET_ID}.impression_logs`
 WHERE 
  is_imp = TRUE
  AND mock_ip_address IN ({response_ips})
GROUP BY 1;

"""


exposed_ip_df = query_bq(exposed_ip_q)

print(f"Rows: {len(exposed_ip_df):,}")
exposed_ip_df.head()

# Transform

# Join responses to impressions to see lift
df_results = df_responses.merge(
    # exposed_ip_df[['mock_ip_address']],
    exposed_ip_df,
    on='mock_ip_address',
    how='left',
    indicator=True
)

df_results['exposed'] = df_results['_merge'] == 'both'
df_results = df_results.drop('_merge', axis=1)

print(f"Exposed: {df_results['exposed'].sum()}, Control: {(~df_results['exposed']).sum()}")
df_results.head()

# Convert date columns to datetime for comparison
df_results['response_dt'] = pd.to_datetime(df_results['response_dt'])
df_results['first_imp_dt'] = pd.to_datetime(df_results['first_imp_dt'])

# Drop rows where the response occurred before or at the same time as first ad impression
# Keeps rows where the condition is met OR where first_imp_dt is null
df_results = df_results[
    (df_results['response_dt'] >= df_results['first_imp_dt']) | 
    (df_results['first_imp_dt'].isna())
]
print(f"Rows after filtering: {len(df_results):,}")

df_results.to_csv("df_results.csv", index=False)

# Load
# Write df_results to BigQuery
table_id = f"{PROJECT_ID}.{DATASET_ID}.lift_results"
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND") # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
job = client.load_table_from_dataframe(df_results, table_id, job_config=job_config)
job.result()
print(f"Loaded {job.output_rows} rows to {table_id}")
