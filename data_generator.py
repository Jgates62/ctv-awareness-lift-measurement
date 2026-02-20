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

# Study configurations: maps response value (1-4) to response_name
STUDIES = {
    1: {
        "name": "Brand_Awareness_Study",
        "question": "How aware are you of this brand?",
        "response_names": {1: "Not Aware", 2: "Somewhat Aware", 3: "Aware", 4: "Very Aware"}
    },
    2: {
        "name": "Brand_Affinity_Study",
        "question": "How do you feel about this brand?",
        "response_names": {1: "Dislike", 2: "Neutral", 3: "Like", 4: "Love"}
    },
    3: {
        "name": "Purchase_Intent_Study",
        "question": "How likely are you to purchase from this brand?",
        "response_names": {1: "Very Unlikely", 2: "Unlikely", 3: "Likely", 4: "Very Likely"}
    }
}

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
    
    # Build mapping of ip -> list of impression datetimes
    ip_to_times = {}
    for ip, times in impressions_df.groupby('mock_ip_address')['date_time']:
        ip_to_times[ip] = [datetime.strptime(t, "%m/%d/%Y %H:%M:%S") for t in times]
    
    # We want some survey takers to be 'Control' (didn't see the ad) 
    # and some 'Exposed' (saw the ad) to measure lift.
    responses = []
    start_date = datetime(2025, 12, 1)
    
    # Mix of IPs: 70% exposed, 30% control (random new IPs)
    for i in range(n_responses):
        is_exposed = random.random() > 0.3
        study_id = random.choice(list(STUDIES.keys()))
        study_config = STUDIES[study_id]
        
        if is_exposed:
            ip = random.choice(exposed_ips)
            # Choose a random impression time for this IP and add 1-1440 minutes
            base_time = random.choice(ip_to_times.get(ip, [start_date]))
            response_time = base_time + timedelta(minutes=random.randint(1, 1440))
            # Simulate Lift: Exposed users more likely to give a 4 (Brand Love)
            val = np.random.choice([1, 2, 3, 4], p=[0.1, 0.2, 0.3, 0.4])
        else:
            ip = f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
            # Random timestamp within December for control
            response_time = start_date + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
            # Control group: Lower probability of a 4
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