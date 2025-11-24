import requests
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from io import StringIO
import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from airflow.hooks.base import BaseHook  # <-- use Airflow connection

# -----------------------------
# CONFIG
# -----------------------------
S3_BUCKET = "mlds430-inaturalist-project"
CHECKPOINT_KEY = "checkpoint.csv"
USERNAME = "arachphotobia" # "arachphotobia"  # default username
# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 client config
config = Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 2})

def checkpoint_key(username):
    return f"checkpoints/{username}_checkpoint.csv"

# -----------------------------
# GET S3 CLIENT FROM AIRFLOW IAM USER CONNECTION
# -----------------------------
def get_s3_client():
    conn = BaseHook.get_connection("aws_default")
    return boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=conn.extra_dejson.get("region_name", "us-east-1"),
        config=config
    )

# -----------------------------
# CHECKPOINT FUNCTIONS
# -----------------------------
def load_checkpoint(username):
    s3_client = get_s3_client()
    key = checkpoint_key(username)
    try:
        logger.info(f"Attempting to load checkpoint from s3://{S3_BUCKET}/{key}")
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(obj['Body'])
        logger.info("Checkpoint loaded successfully.")
        return df["last_created_at"].iloc[0]
    except s3_client.exceptions.NoSuchKey:
        logger.warning("Checkpoint file does not exist in S3. Starting fresh.")
        return None
    except EndpointConnectionError as e:
        logger.error(f"Connection to S3 endpoint timed out: {str(e)}")
    except ClientError as e:
        logger.error(f"AWS Client error occurred: {e.response['Error']['Message']}")
    except Exception as e:
        logger.error(f"Unexpected error loading checkpoint: {str(e)}")
    return None

def save_checkpoint(ts, username):
    s3_client = get_s3_client()
    df = pd.DataFrame([{"last_created_at": ts}])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    key = checkpoint_key(username)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    logger.info(f"Checkpoint saved to s3://{S3_BUCKET}/{key}")

# -----------------------------
# S3 SAVE HELPER
# -----------------------------
def save_csv_incremental_to_s3(df, name):
    s3_client = get_s3_client()
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    key = f"{name}/{name}_{ts}.csv"
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
    print(f"Saved to S3: s3://{S3_BUCKET}/{key}")
    return key

# -----------------------------
# FETCH AND NORMALIZE FUNCTIONS
# -----------------------------
def fetch_user_observations(username, since=None, max_pages=50):
    base_url = "https://api.inaturalist.org/v1/observations"
    results = []

    for page in range(1, max_pages + 1):
        params = {
            "user_id": username,
            "per_page": 200,
            "page": page,
            "order_by": "created_at",
        }
        if since:
            since_dt = datetime.fromisoformat(since) if isinstance(since, str) else since
            since_dt += timedelta(seconds=1)
            params["created_d1"] = since_dt.isoformat()

        r = requests.get(base_url, params=params, timeout=10)
        if r.status_code != 200:
            print(f"Error {r.status_code} on page {page}")
            break

        rows = r.json()["results"]
        if not rows:
            break

        results.extend(rows)
        print(f"Fetched page {page}, total {len(results)})")
        time.sleep(0.5)

    return results

def normalize_staging_tables(observations):
    obs_rows, taxa_rows, users_rows = [], {}, {}
    for obs in observations:
        taxon = obs.get("taxon") or {}
        user = obs.get("user") or {}
        geojson = obs.get("geojson") or {}
        coords = geojson.get("coordinates") or [None, None]

        obs_rows.append({
            "observation_id": obs.get("id"),
            "created_at": obs.get("created_at"),
            "taxon_id": taxon.get("id"),
            "taxon_name": taxon.get("name"),
            "observed_on": obs.get("observed_on"),
            "latitude": coords[1],
            "longitude": coords[0],
            "user_id": user.get("id"),
            "username": user.get("login"),
            "raw_json": json.dumps(obs)
        })

        if taxon.get("id"):
            taxa_rows[taxon["id"]] = {
                "taxon_id": taxon["id"],
                "name": taxon.get("name"),
                "rank": taxon.get("rank"),
                "iconic_taxon_id": taxon.get("iconic_taxon_id"),
                "iconic_taxon_name": taxon.get("iconic_taxon_name")
            }

        if user.get("id"):
            users_rows[user["id"]] = {
                "user_id": user["id"],
                "login": user.get("login"),
                "icon_url": user.get("icon_url"),
            }

    return (
        pd.DataFrame(obs_rows),
        pd.DataFrame(taxa_rows.values()),
        pd.DataFrame(users_rows.values()),
    )

# -----------------------------
# MAIN FUNCTION
# -----------------------------
def main(username=USERNAME, initial=False):
    since = None if initial else load_checkpoint(username)
    observations = fetch_user_observations(username, since)
    if not observations:
        print("No new data.")
        return False
    stg_obs, stg_taxa, stg_users = normalize_staging_tables(observations)

    # create a list of (name, dataframe) tuples
    tables = [
        ("observations", stg_obs),
        ("taxa", stg_taxa),
        ("users", stg_users),
    ]

    for name, df in tables:
        if not df.empty:
            save_csv_incremental_to_s3(df, name)
        else:
            print(f"{name} is empty, nothing to save.")

    latest_ts = stg_obs["created_at"].max()
    save_checkpoint(latest_ts,username)
    
    print(f"Done: {len(stg_obs)} new observations")
    return True


if __name__ == "__main__":
    main(initial=False)
