# streamlit_app.py

import streamlit as st
import pandas as pd
from pyathena import connect
from pyathena.pandas.util import as_pandas


aws_conf = st.secrets["aws"]
conn = connect(
    aws_access_key_id     = aws_conf["aws_access_key_id"],
    aws_secret_access_key = aws_conf["aws_secret_access_key"],
    s3_staging_dir        = aws_conf["s3_staging_dir"],
    region_name           = aws_conf["region_name"],
)




@st.cache_data(ttl=600)
def run_athena_query(sql: str) -> pd.DataFrame:
    """Executes an Athena query and returns a pandas DataFrame."""
    df = as_pandas(conn.execute(sql))
    return df

# ——————————————————————————————————————————————————————————————————————————
# Sidebar navigation
# ——————————————————————————————————————————————————————————————————————————
st.sidebar.title("Navigation")
page = st.sidebar.radio("", ["HOME", "PREDICTIONS", "VERSION"])

# ——————————————————————————————————————————————————————————————————————————
# HOME tab
# ——————————————————————————————————————————————————————————————————————————
if page == "HOME":
    st.title("Welcome to My Athena-Backed Streamlit App")
    st.write(
        """
        Use this lightweight dashboard to:
        - Browse raw tables via Athena
        - Generate on-the-fly predictions
        - Track app version and metadata
        """
    )

# ——————————————————————————————————————————————————————————————————————————
# PREDICTIONS tab
# ——————————————————————————————————————————————————————————————————————————
elif page == "PREDICTIONS":
    st.title("🏷️ Predictions")
    st.markdown("Enter a query to fetch your prediction inputs from Athena:")

    # example: let user choose a table
    table = st.selectbox(
        "Select table",
        options=["your_database.your_schema.your_table1", "your_database.your_schema.your_table2"],
    )
    limit = st.slider("Number of rows", 10, 1000, 100)

    if st.button("Run Query"):
        sql = f"SELECT * FROM {table} LIMIT {limit}"
        df = run_athena_query(sql)
        st.write(f"Returned {len(df)} rows")
        st.dataframe(df)

    # Placeholder for model inference (you’d plug-in your own)
    st.markdown("#### Model output:")
    if st.button("Generate Dummy Prediction"):
        dummy = pd.DataFrame({
            "date": pd.date_range(start=pd.Timestamp.today(), periods=5, freq="D"),
            "predicted_value": pd.np.random.rand(5),
        })
        st.line_chart(dummy.set_index("date"))

# ——————————————————————————————————————————————————————————————————————————
# VERSION tab
# ——————————————————————————————————————————————————————————————————————————
elif page == "VERSION":
    st.title("ℹ️ Version Info")
    # you could load this from a file, env var, or hardcode
    APP_VERSION = "0.1.0"
    st.write(f"**App Version:** {APP_VERSION}")
    st.write(f"**Streamlit Version:** {st.__version__}")
    st.write(f"**AWS Region:** {aws_conf['region_name']}")
