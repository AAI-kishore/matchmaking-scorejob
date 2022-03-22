import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import pickle
import os
import time
import re

from src.likelyhood_model.features import get_features
from src.data.dbutils import required_dfs_for_input
from src.likelyhood_model.preprocess_new_deal import get_processed_job_data
from src.data.dbutils import push_pandas_df_to_gbq

def clean_the_column(x):
    x= x.strip()
    x= x.lower()
    BAD_SYMBOLS_RE = re.compile('[^0-9a-zA-Z_]')
    x= BAD_SYMBOLS_RE.sub('_',x)
    return x

def get_model(deal_role):
    if deal_role in ['Growth Marketer','Paid Social Media Marketer','Email Marketer','Paid Search Marketer']:
        pickle_file = deal_role.replace(" ", "_")+'.pkl'
    else:
        pickle_file = 'Other_roles_combined.pkl'

    pickle_file = os.path.join('models', pickle_file)
    with open(pickle_file,'rb') as f:
        model= pickle.load(f)
    return model

def get_prediction(model,val_data,scoring_data):
    Predicated_var = model.predict(val_data)
    
    val_probs = model.predict_proba(val_data)[:,1] 
    df_val_probs = pd.DataFrame(val_probs, columns = ['pred_prob'])
    
    scoring_data['Predicated_var'] = Predicated_var
    scoring_data['pred_prob'] = df_val_probs
    
    return scoring_data

def main_likelyhood_model(deal_id):
    """
    when deal role is not found with deal id, required_dfs_for_input returns 'role_not_found'
    """
    required_data= required_dfs_for_input(deal_id,model="likelyhood-to-respond")
    if not isinstance(required_data,str):
        deal_role, new_job_data, default_value_df, df_ordinal_ratio, FL_scoring_data, mjf_response= required_data
    else:
        return required_data
    
    jobs_data= get_processed_job_data(new_job_data,default_value_df,df_ordinal_ratio)    
    
    FL_scoring_data['Hourly Pay Rate'] = FL_scoring_data['Hourly Pay Rate'].fillna(int(FL_scoring_data['Hourly Pay Rate'].mean()))
    
    # Joining FL and mjf to get Response Ratio
    FL_scoring_data = FL_scoring_data.merge(mjf_response,left_on = ['Email'],right_on = ['Freelancer Email'],how='left')
    
    FL_scoring_data['Response_Ratio'] = FL_scoring_data['Response_Ratio'].fillna(int(FL_scoring_data['Response_Ratio'].mean()))
    FL_scoring_data = FL_scoring_data[~(FL_scoring_data['Cleaned_Hours_Avail'] == 'Not Accepting New Jobs')]
    
    #Cross join Job data and FL's data
    scoring_data = pd.merge(FL_scoring_data, jobs_data, on ='key').drop("key", 1)
    scoring_data['Price_ratio'] = scoring_data['Hourly Pay Rate']/scoring_data['New_Hourly_budget'].round(2)

    #getting feature columns
    Continuous_var, Final_categorical_var,scaling_var= get_features(deal_role)

    #Scaling Continuous variables
    scaler = MinMaxScaler()

    scaled_model=scaler.fit(scoring_data[scaling_var])
    scoring_data[scaling_var]=scaled_model.transform(scoring_data[scaling_var])

    scoring_data['No Response - Decline Reason (Last 30 Days)'] = scoring_data['No Response - Decline Reason (Last 30 Days)'].apply(lambda x: 1 - x)
    
    #creating epoch_time
    input_epoch_time= int(time.time())
    scoring_data['input_epoch_time']= input_epoch_time

    #changing column names which will be suitable for gbq loading
    columns_list= list(scoring_data.columns)
    rename_columns= [clean_the_column(col) for col in columns_list]
    rename_dict= dict(zip(columns_list,rename_columns))
    scoring_data_gbq= scoring_data.rename(rename_dict,axis=1)
    #preprocessing for loading into gbq
    scoring_data_gbq['ordinal_company_size']=scoring_data_gbq['ordinal_company_size'].astype(float)
    scoring_data_gbq['ordinal_how_soon']=scoring_data_gbq['ordinal_how_soon'].astype(float)
    scoring_data_gbq['ordinal_how_long']=scoring_data_gbq['ordinal_how_long'].astype(float)
    scoring_data_gbq['ordinaldeal_priority']=scoring_data_gbq['ordinaldeal_priority'].astype(float)
    
    #pushing input data for the model to gbq
    push_pandas_df_to_gbq(scoring_data_gbq,"sb_ai","ai_input_likelihood_to_respond")

    #Final Features
    Final_Features = Continuous_var + Final_categorical_var + scaling_var
    
    val_data = scoring_data[Final_Features]

    #getting model
    model= get_model(deal_role)

    #getting predicted scored data
    scoring_data= get_prediction(model,val_data,scoring_data)

    scoring_data = scoring_data.sort_values(by = 'pred_prob', ascending = False).reset_index(drop=True)
    scoring_data['rank'] = scoring_data['pred_prob'].rank(method='min',ascending=False)

    #required columns for api response and gbq output
    req_cols = ['input_epoch_time','Deal ID','Type of Marketer','Email','pred_prob','rank']
    scoring_data= scoring_data[req_cols]
    rename_columns= {
                "Deal ID":"deal_id",
                "Type of Marketer":"type_of_marketer",
                "Email":"email",
                'pred_prob':'pred_prob'
            }
    scoring_data= scoring_data.rename(rename_columns,axis=1)

    return scoring_data
