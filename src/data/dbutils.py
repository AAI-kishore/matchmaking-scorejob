import os
import pandas as pd
import logging
from google.cloud import bigquery

from . import connections

def get_query_from_file(file_name):
    file_name = os.path.join(f'data{os.path.sep}queries', f'{file_name}.sql')
    with open(file_name, 'r') as f:
        query = f.read()
        logging.debug(f"""{file_name}:{query}""")
    return query

def get_change_in_column_names_df(client):
    global cnc_df
    query= get_query_from_file("change_in_column")
    cnc_df= client.query(query).result().to_dataframe(create_bqstorage_client=True)

def change_column_dict(cnc_df,table_name):
    df= cnc_df[cnc_df['gbq_table_name']==table_name].drop('gbq_table_name',axis=1)
    return {v:k for k,v in df.values}

def get_new_job_data(deal_id):
    query= get_query_from_file("new_deal")
    query= query.replace("@deal_id@",f"{deal_id}")
    with connections.get_connection() as connection:
        df= pd.read_sql_query(sql= query,con=connection)
    return df
    
def get_ordinal_value_df(client):
    query= get_query_from_file("ordinal_ratio")
    df= client.query(query).result().to_dataframe(create_bqstorage_client=True)
    df= df.rename(change_column_dict(cnc_df,"ordinal_ratio"),axis=1)
    return df
    
def fetch_deal_role(jobs_data):
    logging.debug(jobs_data.shape[0])
    if(jobs_data.shape[0]>0):
        deal_role = jobs_data['Type of Marketer'].iloc[0]
        logging.debug('Jobs Deal role is ',deal_role)
        return deal_role
    else:
        return "__NotFound__"
    
def get_scoring_FL_data(deal_role, client):
    query= get_query_from_file("scoring_freelancer")
    query= query.replace('variable_role',deal_role)
    df= client.query(query).result().to_dataframe(create_bqstorage_client=True)
    df= df.rename(change_column_dict(cnc_df,"preprocessed_freelancer"),axis=1)
    return df

def get_mjf_response(deal_role,client):
    query= get_query_from_file("scoring_mjf_response")
    query= query.replace('variable_role',deal_role)
    df= client.query(query).result().to_dataframe(create_bqstorage_client=True)
    df= df.rename(change_column_dict(cnc_df,"mjf_response_data"),axis=1)
    return df

def get_default_df(client):
    query= get_query_from_file("default_value")
    df= client.query(query).result().to_dataframe(create_bqstorage_client=True)
    df= df.rename(change_column_dict(cnc_df,"default_value"),axis=1)
    return df

def required_dfs_for_input(deal_id):
    """
    based on jobs data, deal role is fetched. If there is no deal role,
    unable to make scoring data and it simply returns 'deal_role_not_found'
    """
    #getting deal from mysql
    new_job_data= get_new_job_data(deal_id)
    #fetch deal role from input deal data
    deal_role= fetch_deal_role(new_job_data)
    
    if deal_role!='__NotFound__':
        with connections.gbq_client() as client:
            #get change in column name df
            get_change_in_column_names_df(client)
            #get deal mode value from gbq
            default_value_df= get_default_df(client)
            #getting ordinal ratio df
            df_ordinal_ratio= get_ordinal_value_df(client)
            #getting preprocessed freelancers data based on deal role
            FL_scoring_data= get_scoring_FL_data(deal_role, client)
            #getting mjf response data based on deal_role
            mjf_response= get_mjf_response(deal_role,client)
        return deal_role, new_job_data, default_value_df, cnc_df, df_ordinal_ratio, FL_scoring_data, mjf_response
    else:
        return "deal_role_not_found"

def push_pandas_df_to_gbq(df,schema,table_name):
      with connections.gbq_client() as client:

        #writing job config()
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )

        job = client.load_table_from_dataframe(
            df, 
            f"{schema}.{table_name}", 
            job_config=job_config
            )
        job.result() 
