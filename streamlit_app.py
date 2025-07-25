# streamlit_app.py

import streamlit as st
import pandas as pd
from pyathena.pandas.util import as_pandas
import boto3
import time
import plotly.express as px


POLL_INTERVAL = 2

def plot_graph(df:pd.DataFrame, x:str, y:str, title:str):
    fig = px.line(
        df,
        x='date',              # ← use the new date column
        y='price',
        color='tag',           # one line per tag
        line_dash='tag',       # optional: different dash per tag
        title=title,
        labels={'date': 'Date', 'close': 'Close Price', 'tag': 'Tag'}
    )
    
    fig.update_layout(
        xaxis=dict(rangeslider=dict(visible=True)),
        yaxis_type="log",
        margin=dict(l=40, r=40, t=40, b=40)
    )
    st.plotly_chart(fig, use_container_width=True)

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

def orm_asset_query(ticker:str) -> str:
    query = f"""
    with max_date as (
    select max(capture) as capture
    from {st.session_state["database"]+'.'+st.session_state["table"]}
    where ticker = '{ticker}')
    select
        date_capture as "date",
        close as price,
        'real' as tag
    from {st.session_state['silver_table']}
    where partition_0 = '{ticker}'
    and cast(date_capture as date) between date_add('day',-120,current_date) and current_date
    union
    select distinct
        date,
        price_prediction as price,
        'predicted' as tag
    from {st.session_state["database"]+'.'+st.session_state["table"]}
    where capture = (select capture from max_date)
    and ticker = '{ticker}'
    order by "date" asc
    ;"""
    return query


st.title("Portal de forecast v0.1")

st.session_state["aws_key"] = st.secrets["aws_key"]
st.session_state["aws_secret"] = st.secrets["aws_secret"]
st.session_state["region"] = st.secrets["region"]
st.session_state["database"] = st.secrets["database"]
st.session_state["table"] = st.secrets["table"]
st.session_state["silver_table"] = st.secrets["silver_table"]
st.session_state["athena_queries_output"] = st.secrets["athena_queries_output"]

tickers_query = f"""
select distinct partition_0 as ticker
from {st.session_state["database"]+'.'+st.session_state["silver_table"]}
where partition_0 != 'athena_querie_results'
"""

session = boto3.Session(
    aws_access_key_id=st.session_state["aws_key"],
    aws_secret_access_key=st.session_state["aws_secret"],
    region_name=st.session_state["region"] 
)
athena = boto3.client('athena', region_name=st.session_state["region"])
athena = session.client('athena')

tickers_df = run_athena_query(athena, tickers_query, st.session_state['database'], st.session_state["athena_queries_output"] )

col_indc = 0
for index, rows in tickers_df.iterrows():
    query = orm_asset_query(rows['ticker'])
    ticker_dataframe = run_athena_query(athena, query, st.session_state['database'], st.session_state['athena_queries_output'])
    ticker_dataframe['date'] = pd.to_datetime(ticker_dataframe['date'])
    plot_graph(ticker_dataframe, x='date', y='price',title=f'{rows['ticker']}')
    
