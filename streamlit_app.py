# streamlit_app.py

import streamlit as st
import pandas as pd
from pyathena.pandas.util import as_pandas
import boto3
import time

POLL_INTERVAL = 2

def run_athena_query(athena, query: str, database: str, output_location: str) -> pd.DataFrame:


    """
    Execute an Athena query and return results as a pandas DataFrame.
    """
    

    # Start query
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    qid = resp['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(POLL_INTERVAL)

    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        raise RuntimeError(f"Athena query {state}: {reason}")

    paginator = athena.get_paginator('get_query_results')
    pages = paginator.paginate(QueryExecutionId=qid)

    rows = []
    for page in pages:
        for row in page['ResultSet']['Rows']:
            rows.append([col.get('VarCharValue') for col in row['Data']])

    header = rows[0]
    data = rows[1:]
    return pd.DataFrame(data, columns=header)

st.title("Portal de forecast v0.1")

st.session_state["aws_key"] = st.secrets["aws_key"]
st.session_state["aws_secret"] = st.secrets["aws_secret"]
st.session_state["region"] = st.secrets["region"]
st.session_state["database"] = st.secrets["database"]
st.session_state["table"] = st.secrets["table"]
st.session_state["athena_queries_output"] = st.secrets["athena_queries_output"]

session = boto3.Session(
    aws_access_key_id=st.session_state["aws_key"],
    aws_secret_access_key=st.session_state["aws_secret"],
    region_name=st.session_state["region"] 
)
athena = boto3.client('athena', region_name=st.session_state["region"])
athena = session.client('athena')
query = st.text_input("input here")
st.write()
if query:
    df = run_athena_query(
                athena,
                query=query,
                database=st.session_state["database"],
                output_location=st.session_state["athena_queries_output"]
            )
    st.write(df)

