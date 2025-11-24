# <img src="inat_logo.png" alt="iNaturalist Logo" width="100"/> iNaturalist Data Pipeline Overview
## User deep dive: *arachphotobia* <img src="arachphotobia_user.png" alt="arachphotobia profile icon" width="100"/>
iNaturalist Data Pipeline Overview

This project automates an end-to-end data pipeline for iNaturalist observations, combining Apache Airflow orchestration, Fivetran-managed Snowflake ingestion, dbt transformations, and interactive data visualization via Streamlit.

---

## Airflow Project Structure

- `dags/`: Airflow DAGs (primary: `inaturalist_pipeline`)  
- `include/`: Custom Python modules for API pulls, Fivetran sync, and dbt project  
- `streamlit_app/`: Streamlit visualization application  
- `requirements.txt`: Python dependencies  
- Other configs and plugins

Run Airflow locally using `astro dev start`. Access the UI at [http://localhost:8080](http://localhost:8080). Configure connections (`aws_default`, `snowflake_default`, `fivetran_default`) through the Airflow UI.

---

## inaturalist_pipeline DAG

A daily linear workflow:

1. **Fetch iNaturalist data and stage CSVs to S3 (`run_initial_run_py`)**  
2. **Trigger Fivetran sync to load data into Snowflake (`fivetran_sync`)**  
3. **Run dbt models to build analytics tables (`run_dbt_transform`)**

---

## dbt Project Highlights

- Located at `include/inaturalist_project`  
- Models 7 objects into Snowflake schema `FIVETRAN_DATABASE.S3_BULLFROG`  
- Staging views normalize raw observations, taxa, and users  
- Core tables: observation facts, taxon and user dimensions, and feature-engineered observation features with rolling counts and taxon flags

---

## Streamlit Visualization App

Located under `streamlit_app/`, this app enables interactive exploration of Snowflake data post-pipeline.

### Features

- Connects securely to Snowflake using a private key  
- Sidebar filters for iconic taxon and date range  
- Interactive tabs with:
  - Visualizations: time series plots of `Aves` vs `Insecta` rolling counts, seasonal averages, and taxon observation percentages  
  - Raw Data: tabular view of loaded data from Snowflake  
  - Data Quality: simple metric indicating completeness of data  

### Running the App

Set up a Python environment, install dependencies, then run:

`streamlit run streamlit_app/app.py`

### Data Source

The app queries the `FCT_OBSERVATION_FEATURES` and `FCT_OBSERVATIONS` dbt models in Snowflake, filtering based on selected dates and taxa.

---
### Configuration & Dependencies

- **Airflow Connections:**

  - `aws_default` for AWS S3 access:  
    This connection must be configured with credentials for an IAM user or role that has **read and write permissions** on the specific S3 bucket used by the pipeline, e.g., `mlds430-inaturalist-project`.  
    The IAM policy should grant at least:  
    - `s3:ListBucket` on the bucket ARN:  
      `arn:aws:s3:::mlds430-inaturalist-project`  
    - `s3:GetObject` and `s3:PutObject` on all objects in the bucket:  
      `arn:aws:s3:::mlds430-inaturalist-project/*`  
    Configure the connection in Airflow UI (**Admin > Connections**) with:  
    - **Conn Id**: `aws_default`  
    - **Conn Type**: `Amazon Web Services`  
    - **Login**: AWS Access Key ID  
    - **Password**: AWS Secret Access Key  

- **Python packages:**  
  `apache-airflow`, `requests`, `pandas`, `boto3`, `botocore`, `streamlit`, `plotly`, `cryptography`

- **Environment paths:**  
  - Airflow home directory with dbt project at `include/inaturalist_project`  
  - Streamlit app files in `streamlit_app/`

- ****Data Source Configuration**
- The pipeline by default fetches iNaturalist observations for the user arachphotobia. If you want to extract data for a different user, you will need to modify the username parameter in the Airflow DAG or in the Python script include.inat_api_pull.



