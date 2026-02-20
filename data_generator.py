import random
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")

# Only run once to create dataset, then comment out or remove
# client = bigquery.Client(project=project_id)
# dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
# dataset.location = "US"
# client.create_dataset(dataset, exists_ok=True)
# print(f"Dataset {dataset_id} ready")


# Configuration
NUM_IMPRESSIONS = 10000
NUM_UNIQUE_IPS = 5000
CAMPAIGNS = [
    (101, "Stranger_Things_S5_Launch"),
    (102, "Netflix_Standard_With_Ads_Promo"),
    (103, "Bridgerton_Brand_Awareness")
]

# 1. Generate Unique IP Pool
ip_pool = [f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}" 
           for _ in range(NUM_UNIQUE_IPS)]

# 2. Generate Impression Logs
def generate_impression_logs(n):
    data = []
    start_date = datetime(2025, 12, 1)
    
    for _ in range(n):
        camp_id, camp_name = random.choice(CAMPAIGNS)
        # Random timestamp within December
        dt = start_date + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        # Funnel Logic: Most start, some complete
        is_imp = True
        ad_started = random.random() > 0.05  # 95% start rate
        ad_completed = ad_started and (random.random() > 0.15) # 85% of starts complete
        
        data.append({
            "ad_campaign_id": camp_id,
            "ad_campaign_name": camp_name,
            "date_time": dt.strftime("%m/%d/%Y %H:%M:%S"),
            "mock_ip_address": random.choice(ip_pool),
            "ad_cost": random.randint(15000, 25000), # Microcurrency (e.g. $0.015 - $0.025)
            "is_imp": is_imp,
            "ad_started": ad_started,
            "ad_completed": ad_completed
        })
    return pd.DataFrame(data)

# 3. Generate Survey Responses
def generate_responses(impressions_df, n_responses=1000):
    # Get unique IPs from impressions to simulate 'Exposed' group
    exposed_ips = impressions_df['mock_ip_address'].unique()
    
    # We want some survey takers to be 'Control' (didn't see the ad) 
    # and some 'Exposed' (saw the ad) to measure lift.
    responses = []
    
    # Mix of IPs: 70% exposed, 30% control (random new IPs)
    for i in range(n_responses):
        is_exposed = random.random() > 0.3
        if is_exposed:
            ip = random.choice(exposed_ips)
            # Simulate Lift: Exposed users more likely to give a 4 (Brand Love)
            val = np.random.choice([1, 2, 3, 4], p=[0.1, 0.2, 0.3, 0.4])
        else:
            ip = f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
            # Control group: Lower probability of a 4
            val = np.random.choice([1, 2, 3, 4], p=[0.2, 0.3, 0.3, 0.2])
            
        responses.append({
            "mock_ip_address": ip,
            "response_value": val
        })
    return pd.DataFrame(responses)

# Execute
df_impressions = generate_impression_logs(NUM_IMPRESSIONS)
df_responses = generate_responses(df_impressions, 1200)

# Save to CSV
df_impressions.to_csv("impression_logs.csv", index=False)
df_responses.to_csv("responses.csv", index=False)

print(f"Success! Generated {len(df_impressions)} impressions and {len(df_responses)} responses.")

def write_to_bigquery(df, table_name):
    destination = f"{project_id}.{dataset_id}.{table_name}"
    df.to_gbq(
        destination_table=f"{dataset_id}.{table_name}",
        project_id=project_id,
        if_exists="replace"  # options: 'replace', 'append', 'fail'
    )
    print(f"✅ Successfully wrote {len(df)} rows to {destination}")

# Replace your to_csv() calls with:
write_to_bigquery(df_impressions, "impression_logs")
write_to_bigquery(df_responses, "survey_responses")