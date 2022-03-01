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

def get_scoring_jobs_data(deal_id,conn):
    query= get_query_from_file("scoring_jobs")
    query= query.replace('variable_deal_id',f'{deal_id}')
    df= pd.read_sql_query(sql= query,con=conn)
    return df

def fetch_deal_role(jobs_data):
    logging.debug(jobs_data.shape[0])
    if(jobs_data.shape[0]>0):
        deal_role = jobs_data['Type of Marketer'].iloc[0]
        logging.debug('Jobs Deal role is ',deal_role)
        return deal_role
    else:
        return "__NotFound__"
    
def get_scoring_FL_data(deal_role, conn):
    query= get_query_from_file("scoring_freelancer")
    query= query.replace('variable_role',deal_role)
    df= pd.read_sql_query(sql= query,con=conn)
    return df

def get_mjf_response(deal_role,conn):
    query= get_query_from_file("scoring_mjf_response")
    query= query.replace('variable_role',deal_role)
    df= pd.read_sql_query(sql= query,con=conn)
    return df

def required_dfs_for_input(deal_id):
    """
    based on jobs data, deal role is fetched. If there is no deal role,
    unable to make scoring data and it simply returns 'deal_role_not_found'
    """
    with connections.get_connection() as conn:
        jobs_data= get_scoring_jobs_data(deal_id,conn)
        deal_role= fetch_deal_role(jobs_data)
        if deal_role!='__NotFound__':
            FL_scoring_data= get_scoring_FL_data(deal_role, conn)
            mjf_response= get_mjf_response(deal_role,conn)
            return deal_role, jobs_data, FL_scoring_data, mjf_response
        else:
            return "deal_role_not_found"
    