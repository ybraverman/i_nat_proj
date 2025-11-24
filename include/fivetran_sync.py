# include/fivetran_sync.py
from airflow.hooks.base import BaseHook
import base64
import requests

def run_fivetran_sync(connector_id: str):
    """
    Trigger a Fivetran sync using the Airflow connection named 'fivetran_default'.
    """

    # Pull key/secret from Airflow (NOT in code)
    conn = BaseHook.get_connection("fivetran_default")

    key = conn.login
    secret = conn.password
    host = conn.host or "https://api.fivetran.com"

    # Basic auth header
    auth_header = base64.b64encode(f"{key}:{secret}".encode()).decode()
    headers = {"Authorization": f"Basic {auth_header}"}

    # Correct Fivetran endpoint
    url = f"{host}/v1/connectors/{connector_id}/force"

    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()

    return f"Triggered Fivetran sync for {connector_id}"
