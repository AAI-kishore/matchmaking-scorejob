import os
import pandas as pd
import logging

from . import connections

def get_query_from_file(file_name):
    file_name = os.path.join(f'data{os.path.sep}queries', f'{file_name}.sql')
    with open(file_name, 'r') as f:
        query = f.read()
        logging.debug(f"""{file_name}:{query}""")
    return query

# def get_scoring_jobs_data(deal_id,conn):
#     query= get_query_from_file("scoring_jobs")
#     query= query.replace('variable_deal_id',f'{deal_id}')
#     df= pd.read_sql_query(sql= query,con=conn)
#     return df

# def fetch_deal_role(jobs_data):
#     logging.debug(jobs_data.shape[0])
#     if(jobs_data.shape[0]>0):
#         deal_role = jobs_data['Type of Marketer'].iloc[0]
#         logging.debug('Jobs Deal role is ',deal_role)
#         return deal_role
#     else:
#         return "__NotFound__"
    
# def get_scoring_FL_data(deal_role, conn):
#     query= get_query_from_file("scoring_freelancer")
#     query= query.replace('variable_role',deal_role)
#     df= pd.read_sql_query(sql= query,con=conn)
#     return df

# def get_mjf_response(deal_role,conn):
#     query= get_query_from_file("scoring_mjf_response")
#     query= query.replace('variable_role',deal_role)
#     df= pd.read_sql_query(sql= query,con=conn)
#     return df
def get_change_in_column_names_df(client):
    global cnc_df
    query= get_query_from_file("change_in_column")
    cnc_df= client.query(query).result().to_dataframe(create_bqstorage_client=True)

def change_column_dict(cnc_df,table_name):
    df= cnc_df[cnc_df['gbq_table_name']==table_name].drop('gbq_table_name',axis=1)
    return {v:k for k,v in df.values}

def get_scoring_jobs_data(deal_id,client):
    query= get_query_from_file("scoring_jobs")
    query= query.replace('variable_deal_id',f'{deal_id}')
    df= client.query(query).result().to_dataframe(create_bqstorage_client=True)
    df= df.rename(change_column_dict(cnc_df,"preprocessed_deals"),axis=1)
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


# def required_dfs_for_input(deal_id):
#     """
#     based on jobs data, deal role is fetched. If there is no deal role,
#     unable to make scoring data and it simply returns 'deal_role_not_found'
#     """
#     with connections.get_connection() as conn:
#         jobs_data= get_scoring_jobs_data(deal_id,conn)
#         deal_role= fetch_deal_role(jobs_data)
#         if deal_role!='__NotFound__':
#             FL_scoring_data= get_scoring_FL_data(deal_role, conn)
#             mjf_response= get_mjf_response(deal_role,conn)
#             return deal_role, jobs_data, FL_scoring_data, mjf_response
#         else:
#             return "deal_role_not_found"

def required_dfs_for_input(deal_id):
    """
    based on jobs data, deal role is fetched. If there is no deal role,
    unable to make scoring data and it simply returns 'deal_role_not_found'
    """
    with connections.gbq_client() as client:
        #get change in column name df
        get_change_in_column_names_df(client)
        
        jobs_data= get_scoring_jobs_data(deal_id,client)
        deal_role= fetch_deal_role(jobs_data)
        if deal_role!='__NotFound__':
            FL_scoring_data= get_scoring_FL_data(deal_role, client)
            mjf_response= get_mjf_response(deal_role,client)
            return deal_role, jobs_data, FL_scoring_data, mjf_response
        else:
            return "deal_role_not_found"
    