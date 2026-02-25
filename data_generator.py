"""
Data Generator: Synthetic CTV Ads & Brand Lift Study
Author: Jordan Gates

Purpose:
This script generates synthetic data to simulate programmatic bidstream data (impressions) 
and survey response data to enable measurement of Brand Lift.

Key Features:
- Foundational Metadata: Creates a mapping between studies and ad campaigns.
- Impression Logic: Simulates a standard CTV ad funnel (Start -> Completion).
- Experiment Design: Generates 'Exposed' and 'Control' groups with simulated 
  brand sentiment lift.
"""

import random
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
import json

# -------------------------------------------------------------------------
# 1. Environment & Global Configuration
# -------------------------------------------------------------------------
load_dotenv()

# Load credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

# Dataset Scale Configuration
NUM_IMPRESSIONS = 50000
NUM_UNIQUE_IPS = 20000

# Metadata Schema: Defines the relationship between Studies and Campaigns
# This mapping is critical for the UNNEST operations in downstream ELT scripts.
STUDIES = {
    1: {
        "name": "Brand_Awareness_Study",
        "question": "How aware are you of this brand?",
        "response_names": {1: "Not Aware", 2: "Somewhat Aware", 3: "Aware", 4: "Very Aware"},
        "campaigns": {101: "Brand Awareness VA", 102: "Brand Awareness NC"}
    },
    2: {
        "name": "Brand_Affinity_Study",
        "question": "How do you feel about this brand?",
        "response_names": {1: "Dislike", 2: "Neutral", 3: "Like", 4: "Love"},
        "campaigns": {103: "National Brand Affinity"}
    },
    3: {
        "name": "Purchase_Intent_Study",
        "question": "How likely are you to purchase from this brand?",
        "response_names": {1: "Very Unlikely", 2: "Unlikely", 3: "Likely", 4: "Very Likely"},
        "campaigns": {104: "Initial Awareness Campaign 18-24", 105: "Initial Awareness Campaign 25-34"}
    }
}

# -------------------------------------------------------------------------
# 2. Synthetic Data Generation Logic
# -------------------------------------------------------------------------

def generate_study_campaign_table(studies):
    """Generates the dimensional metadata table mapping studies to campaign arrays."""
    rows = []
    for study_id, details in studies.items():
        campaign_ids = list(details.get("campaigns", {}).keys())
        rows.append({
            "study_id": study_id,
            "name": details["name"],
            "question": details["question"],
            "response_names": json.dumps(details["response_names"]),
            "campaigns_json": json.dumps(details["campaigns"]),
            "campaign_id_array": campaign_ids 
        })
    return pd.DataFrame(rows)

# Generate a pool of unique IPs to simulate the reachable audience
ip_pool = [f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}" 
           for _ in range(NUM_UNIQUE_IPS)]

def generate_impression_logs(n):
    """Simulates raw CTV ad impression logs with basic funnel metrics."""
    data = []
    start_date = datetime(2025, 12, 1)
    
    campaign_pairs = []
    for study in STUDIES.values():
        for camp_id, camp_name in study["campaigns"].items():
            campaign_pairs.append((camp_id, camp_name))

    for _ in range(n):
        camp_id, camp_name = random.choice(campaign_pairs)
        dt = start_date + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        # Simulating ad performance metrics (Start Rate vs. Completion Rate)
        is_imp = True
        ad_started = random.random() > 0.05  
        ad_completed = ad_started and (random.random() > 0.15) 
        
        data.append({
            "ad_campaign_id": camp_id,
            "ad_campaign_name": camp_name,
            "date_time": dt.strftime("%m/%d/%Y %H:%M:%S"),
            "mock_ip_address": random.choice(ip_pool),
            "ad_cost": random.randint(15000, 25000), 
            "is_imp": is_imp,
            "ad_started": ad_started,
            "ad_completed": ad_completed
        })
    return pd.DataFrame(data)

def generate_responses(impressions_df, n_responses=1000):
    """Simulates survey completion data with a pre-defined brand lift signal."""
    exposed_ips = impressions_df['mock_ip_address'].unique()
    
    ip_to_times = {}
    for ip, times in impressions_df.groupby('mock_ip_address')['date_time']:
        ip_to_times[ip] = [datetime.strptime(t, "%m/%d/%Y %H:%M:%S") for t in times]
    
    responses = []
    start_date = datetime(2025, 12, 1)
    
    for i in range(n_responses):
        is_exposed = random.random() > 0.3
        study_id = random.choice(list(STUDIES.keys()))
        study_config = STUDIES[study_id]
        
        if is_exposed:
            ip = random.choice(exposed_ips)
            base_time = random.choice(ip_to_times.get(ip, [start_date]))
            response_time = base_time + timedelta(minutes=random.randint(1, 1440))
            # Simulating Positive Lift: Exposed users have higher probability of positive sentiment (4)
            val = np.random.choice([1, 2, 3, 4], p=[0.1, 0.2, 0.3, 0.4])
        else:
            # Control Group IPs use a specific subnet for clear identification during testing
            ip = f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
            response_time = start_date + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
            # Baseline Sentiment: Control group follows a more neutral distribution
            val = np.random.choice([1, 2, 3, 4], p=[0.2, 0.3, 0.3, 0.2])
            
        responses.append({
            "mock_ip_address": ip,
            "response_value": val,
            "response_name": study_config["response_names"][val],
            "measurement_study_id": study_id,
            "question": study_config["question"],
            "date_time": response_time.strftime("%m/%d/%Y %H:%M:%S"),
            "positive_response": val >= 3
        })
    return pd.DataFrame(responses)

# -------------------------------------------------------------------------
# 3. Data Ingestion (BigQuery)
# -------------------------------------------------------------------------

def write_to_bigquery(df, table_name):
    """
    Loads a DataFrame into BigQuery using the official google-cloud-bigquery client.
    
    Using the LoadJobConfig with WRITE_TRUNCATE ensures idempotency during 
    pipeline development and testing.
    """
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE is used to replace existing data from previous runs.
        write_disposition="WRITE_TRUNCATE",
    )

    print(f"Starting load job for {table_id}...")
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() # Wait for the upload to complete
        print(f"Successfully loaded {len(df):,} rows to {table_id}")
    except Exception as e:
        print(f"Error loading to BigQuery: {e}")
        raise

# Execution Flow
if __name__ == "__main__":
    # Generate DataFrames
    df_study_campaigns = generate_study_campaign_table(STUDIES)
    df_impressions = generate_impression_logs(NUM_IMPRESSIONS)
    df_responses = generate_responses(df_impressions, 15000)

    # Persist to BigQuery
    write_to_bigquery(df_study_campaigns, "study_campaigns")
    write_to_bigquery(df_impressions, "impression_logs")
    write_to_bigquery(df_responses, "survey_responses")
    
    print("\nAll tables successfully provisioned in BigQuery.")