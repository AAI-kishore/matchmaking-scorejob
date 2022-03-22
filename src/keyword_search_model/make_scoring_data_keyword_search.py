import pandas as pd
import spacy
import os
import logging
import time
import re

from src.data.dbutils import required_dfs_for_input
from src.keyword_search_model.entity_extraction_from_jobs_data import extract_entities_from_jobs_data
from src.data.dbutils import push_pandas_df_to_gbq

def clean_the_column(x):
    x= x.strip()
    x= x.lower()
    BAD_SYMBOLS_RE = re.compile('[^0-9a-zA-Z_]')
    x= BAD_SYMBOLS_RE.sub('_',x)
    return x

def get_FL_scoring_data(FL_scoring_data):
    #filtering on cleaned hours avail
    FL_scoring_data = FL_scoring_data[~(FL_scoring_data['Cleaned_Hours_Avail'] == 'Not Accepting New Jobs')]
    select_columns= [
        "key",
        "Email",
        "FL_Tools_Platform_Entities_NER",
        "FL_Industry_Entities_NER",
        "FL_Strategy_Entities_NER",
        "FL_Skills_Entities_NER"
    ]
    fl_entities_extracted_data= FL_scoring_data[select_columns]
    return fl_entities_extracted_data

def similarity_score(FL_col,Jobs_col):
    score = 0
    FL_tokens = FL_col.split(",")
    Jobs_tokens = Jobs_col.split(",")
    Total_Job_Tokens = len(Jobs_tokens)
    if (len(Jobs_col.strip())) > 0:
        Matching_Tokens = 0
        for i in Jobs_tokens:
            for j in FL_tokens:
                if j == i:
                    Matching_Tokens = Matching_Tokens + 1
        score =  float(Matching_Tokens/Total_Job_Tokens)
    else:
        score = 0
    return score

def get_entity_weightages():
    Tools_Weightage = 0.2
    Industry_Weightage = 0.45
    Strategy_Weightage = 0.05
    Skills_Weightage = 0.3
    return Tools_Weightage,Industry_Weightage,Strategy_Weightage,Skills_Weightage

def generate_fl_score_data_for_extracted_keyword(new_job_data,FL_scoring_data):
    #getting extracted entities job data
    jobs_data_entities_extracted= extract_entities_from_jobs_data(new_job_data)
    #getting extracted entities freelancers data
    fl_entities_extracted_data= get_FL_scoring_data(FL_scoring_data)

    #merging all the data
    mf= pd.merge(jobs_data_entities_extracted,fl_entities_extracted_data,on='key').drop('key',axis=1)
    #null treatment
    for i in mf.columns:
        mf[i]= mf[i].fillna(" ")
    
    #pushing input data to gbq
    #creating epoch_time
    input_epoch_time= int(time.time())
    mf['input_epoch_time']= input_epoch_time

    #changing column names which will be suitable for gbq loading
    columns_list= list(mf.columns)
    rename_columns= [clean_the_column(col) for col in columns_list]
    rename_dict= dict(zip(columns_list,rename_columns))
    mf_gbq= mf.rename(rename_dict,axis=1)
    
    #pushing input data for the model to gbq
    push_pandas_df_to_gbq(mf_gbq,"sb_ai","ai_input_keyword_search")

    #getting entity weightages
    Tools_Weightage,Industry_Weightage,Strategy_Weightage,Skills_Weightage= get_entity_weightages()

    #calculating similarity score for each entity corresponding to deal and freelancer data
    mf['Tools_score'] = mf.apply(lambda x: similarity_score(x['FL_Tools_Platform_Entities_NER'],x['Jobs_Tools_Platform_Entities_NER']),axis = 1)
    mf['Industry_score'] = mf.apply(lambda x: similarity_score(x['FL_Industry_Entities_NER'],x['Jobs_Industry_Entities_NER']),axis = 1)
    mf['Strategy_score'] = mf.apply(lambda x: similarity_score(x['FL_Strategy_Entities_NER'],x['Jobs_Strategy_Entities_NER']),axis = 1)
    mf['Skills_score'] = mf.apply(lambda x: similarity_score(x['FL_Skills_Entities_NER'],x['Jobs_Skills_Entities_NER']),axis = 1)

    #calculating weightage score based on strategy
    strategy = jobs_data_entities_extracted['Jobs_Strategy_Entities_NER'].iloc[0]
    len_Strategy = len(strategy.strip())
    value = 4
    if len_Strategy > 0:
        weight_value = 1
        #value = 4
    else:
        weight_value = 0.95
        #value = 3

    mf['score_keyword_search'] = (mf['Tools_score'] * Tools_Weightage) + (mf['Industry_score']*Industry_Weightage) + (mf['Strategy_score']*Strategy_Weightage) + (mf['Skills_score']*Skills_Weightage)
    mf['score_keyword_search'] = mf['score_keyword_search']/weight_value

    #calculating rank based on Final score
    mf['rank_keyword_search']= mf['score_keyword_search'].rank(method='min',ascending=False)

    #select required columns
    mf= mf[[
        'Deal ID',
        'Type of Marketer',
        'Email',
        "score_keyword_search",
        "rank_keyword_search",
        "input_epoch_time"
        ]]
    
    rename_columns= {
                "Deal ID":"deal_id",
                "Type of Marketer":"type_of_marketer",
                "Email":"email"
            }
    mf= mf.rename(rename_columns,axis=1)
    
    mf= mf.sort_values(by='rank_keyword_search').reset_index(drop=True)
    
    return mf

def main_keyword_search(deal_id):
    """
    when deal role is not found with deal id, required_dfs_for_input returns 'role_not_found'
    """
    required_data= required_dfs_for_input(deal_id,model="keyword-search")
    if not isinstance(required_data,str):
        new_job_data,FL_scoring_data= required_data
    else:
        return required_data
    
    scoring_data= generate_fl_score_data_for_extracted_keyword(new_job_data,FL_scoring_data)

    return scoring_data