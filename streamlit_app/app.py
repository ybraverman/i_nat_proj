import streamlit as st
import pandas as pd
from snowflake.connector import connect
import plotly.express as px
import plotly.graph_objects as go
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datetime import datetime

# -----------------------------
# Load RSA private key for Snowflake authentication
# -----------------------------
key_path = os.path.join(os.path.dirname(__file__), "../include/rsa_key.pem")
with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )

# -----------------------------
# Configure Streamlit page
# -----------------------------
st.set_page_config(page_title="iNaturalist Explorer", layout="wide")

# -----------------------------
# Snowflake connection
# Using @st.cache_resource to reuse connection across reruns
# -----------------------------
@st.cache_resource
def get_snowflake_connection():
    return connect(
        user='BULLFROG',
        account='azb79167',
        private_key=private_key,
        warehouse='TRAINING_WH',
        database='FIVETRAN_DATABASE',
        schema='S3_BULLFROG'
    )

# -----------------------------
# Sidebar: Filters
# -----------------------------
iconic_taxa = ["Aves", "Amphibia", "Animalia", "Actinopterygii", "Chromista",
               "Insecta","Fungi","Mammalia","Mollusca","Reptilia","Plantae"]
default_taxon = "Aves"

st.sidebar.header("Filters")
selected_taxon = st.sidebar.selectbox(
    "Select Iconic Taxon for Visualization", 
    iconic_taxa, 
    index=iconic_taxa.index(default_taxon)
)
min_date = st.sidebar.date_input("Start Date", value=pd.Timestamp("2023-01-01"))
max_date = st.sidebar.date_input("End Date", value=pd.Timestamp("2023-12-31"))

# -----------------------------
# Fetch list of unique usernames from Snowflake
# Cached for 1 hour
# -----------------------------
@st.cache_data(ttl=3600)
def get_usernames():
    conn = get_snowflake_connection()
    query = "SELECT DISTINCT USERNAME FROM DIM_USERS ORDER BY USERNAME"
    usernames = pd.read_sql(query, conn)['USERNAME'].tolist()
    return usernames

usernames = get_usernames()
selected_user = st.sidebar.selectbox("Select User", usernames)

# -----------------------------
# Load observations for the selected user and date range
# Cached for 1 hour to avoid repeated queries
# -----------------------------
@st.cache_data(ttl=3600)
def load_data(min_date, max_date, user):
    conn = get_snowflake_connection()
    query = f"""
        SELECT 
            ofe.*,
            obs.LATITUDE,
            obs.LONGITUDE
        FROM FCT_OBSERVATION_FEATURES ofe
        JOIN FCT_OBSERVATIONS obs
            ON ofe.OBSERVATION_ID = obs.OBSERVATION_ID
            AND ofe.USER_ID = obs.USER_ID
        JOIN DIM_USERS u
            ON ofe.USER_ID = u.USER_ID
        WHERE ofe.OBSERVED_AT BETWEEN '{min_date}' AND '{max_date}' 
          AND u.USERNAME = '{user}'
        ORDER BY ofe.OBSERVED_AT DESC
    """
    df = pd.read_sql(query, conn)
    return df

# -----------------------------
# Main title and description
# -----------------------------
st.title(f"iNaturalist Explorer: {selected_user} Snapshot")
st.markdown("**Interactive exploration of iNaturalist data from Snowflake**")

# Load data for selected user
df = load_data(min_date, max_date, selected_user)

# -----------------------------
# Update date range filter based on actual data
# -----------------------------
if len(df) > 0:
    min_date = df['OBSERVED_AT'].min().date()
    max_date = df['OBSERVED_AT'].max().date()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Filter dataframe by date range
    if len(date_range) == 2:
        df = df[
            (df['OBSERVED_AT'].dt.date >= date_range[0]) & 
            (df['OBSERVED_AT'].dt.date <= date_range[1])
        ]

# -----------------------------
# Display metrics
# -----------------------------
st.markdown("---")
col1, col2, col3= st.columns(3)
with col1:
    st.metric("Observations Range", f"{min_date} to {max_date}")
with col2:
    st.metric("Total Observations", f"{df['OBSERVATION_ID'].nunique():,}")
with col3:
    st.metric(f"Total {selected_taxon} Observations", 
              f"{df[df['ICONIC_TAXON_NAME'] == selected_taxon].shape[0]:,}")
st.markdown("---")

# -----------------------------
# Tabs for visualizations, raw data, and data quality
# -----------------------------
tab1, tab2, tab3 = st.tabs(["📊 Visualizations", "📋 Raw Data", "✅ Data Quality"])

# -----------------------------
# Tab 1: Visualizations
#  - Rolling 30-day counts for Aves and Insecta
#  - Seasonal average by day of year
#  - Histogram of observations by taxon with selected taxon highlighted
# -----------------------------
with tab1:
    # Time series plot
    with st.container():
        st.subheader("Insects 🪲 vs Aves 🐦 Observed Over Time")
        fig_full = go.Figure()
        fig_full.add_trace(go.Scatter(
            x=df['OBSERVED_AT'], 
            y=df['ROLLING_AVES_30'], 
            name='🐦',
            hovertemplate='🐦<br>Date: %{x|%Y-%m-%d}<br>Rolling 30 day Count: %{y}<extra></extra>'
        ))
        fig_full.add_trace(go.Scatter(
            x=df['OBSERVED_AT'], 
            y=df['ROLLING_INSECTA_30'], 
            name='🪲',
            hovertemplate='🪲<br>Date: %{x|%Y-%m-%d}<br>Rolling 30 day Count: %{y}<extra></extra>'
        ))
        fig_full.update_layout(
            xaxis=dict(tickfont=dict(size=18)),
            hoverlabel=dict(font_size=16)
        )
        st.plotly_chart(fig_full, use_container_width=True)

    # Seasonal average plot
    df['month_day'] = df['OBSERVED_AT'].dt.strftime('%m-%d')
    seasonal_avg = df.groupby('month_day').agg({'ROLLING_AVES_30':'mean', 'ROLLING_INSECTA_30':'mean'}).reset_index()
    seasonal_avg['date'] = pd.to_datetime('2000-' + seasonal_avg['month_day'])

    with st.container():
        st.subheader("Average 🪲 vs 🐦 Observations by Season (Across Years)")
        fig_years = go.Figure()
        fig_years.add_trace(go.Scatter(
            x=seasonal_avg['date'], 
            y=seasonal_avg['ROLLING_AVES_30'], 
            name='🐦',
            hovertemplate='🐦<br>Date: %{x|%Y-%m-%d}<br>Av Rolling 30 day Count: %{y}<extra></extra>'

        ))
        fig_years.add_trace(go.Scatter(
            x=seasonal_avg['date'], 
            y=seasonal_avg['ROLLING_INSECTA_30'], 
            name='🪲',
            hovertemplate='🪲<br>Date: %{x|%Y-%m-%d}<br>Av Rolling 30 day Count: %{y}<extra></extra>'

        ))
        fig_years.update_layout(
            xaxis=dict(tickformat='%b', tickfont=dict(size=18)),
            hoverlabel=dict(font_size=16)
        )
        st.plotly_chart(fig_years, use_container_width=True)

    # Histogram of observations by taxon
    count_by_taxon = df['ICONIC_TAXON_NAME'].value_counts(normalize=True).mul(100).reset_index()
    count_by_taxon.columns = ['Taxon', 'Percentage']
    colors = ["#FFD700" if taxon == selected_taxon else "#7FDBFF" for taxon in count_by_taxon['Taxon']]
    fig_hist = px.bar(
        count_by_taxon,
        x='Taxon',
        y='Percentage',
        title="Observation Percentage by Taxon",
        color='Taxon',
        color_discrete_sequence=colors,
        labels={'Percentage': '% of Observations'}
    )
    fig_hist.update_layout(
        title_font=dict(size=24),
        xaxis=dict(title_font=dict(size=20), tickfont=dict(size=16)),
        yaxis=dict(title_font=dict(size=20), tickfont=dict(size=16)),
        hoverlabel=dict(font_size=18, font_family="Arial")
    )

    # Annotate selected taxon
    selected_percentage = count_by_taxon.loc[count_by_taxon['Taxon'] == selected_taxon, 'Percentage'].values
    if selected_percentage.size > 0:
        selected_value = selected_percentage[0]
        fig_hist.add_annotation(
            x=selected_taxon,
            y=selected_value,
            text=f"{selected_value:.2f}%",
            showarrow=True,
            arrowhead=2,
            yshift=10,
            font=dict(color="black", size=18)
        )
        fig_hist.update_layout(xaxis=dict(tickfont=dict(size=18)))
    st.plotly_chart(fig_hist, use_container_width=True)

# -----------------------------
# Tab 2: Display raw dataframe
# -----------------------------
with tab2:
    st.dataframe(df)

# -----------------------------
# Tab 3: Data quality metric (percentage of non-null values)
# -----------------------------
with tab3:
    data_quality = (1 - df.isnull().sum().sum() / df.size) * 100
    st.metric("Data Quality", f"{data_quality:.1f}%")
