import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import pickle
import os

from src.inference.features import get_features
from src.data.dbutils import required_dfs_for_input
from src.inference.preprocess_new_deal import get_processed_job_data

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
    
    scoring_data = scoring_data.sort_values(by = 'pred_prob', ascending = False).reset_index()
    scoring_data['Rank'] = scoring_data['pred_prob'].rank(method='min',ascending=False)
    
    return scoring_data


def main(deal_id):
    """
    when deal role is not found with deal id, required_dfs_for_input returns 'role_not_found'
    """
    required_data= required_dfs_for_input(deal_id)
    if not isinstance(required_data,str):
        deal_role, new_job_data, default_value_df, cnc_df, df_ordinal_ratio, FL_scoring_data, mjf_response= required_data
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
    
    #Final Features
    Final_Features = Continuous_var + Final_categorical_var + scaling_var
    
    val_data = scoring_data[Final_Features]

    #getting model
    model= get_model(deal_role)

    #getting predicted scored data
    scoring_data= get_prediction(model,val_data,scoring_data)

    return scoring_data

