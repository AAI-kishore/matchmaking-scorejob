from flask import Flask, request
import pandas as pd
import joblib
import xgboost as xgb
import json
from datetime import datetime

import logging
logging.basicConfig(
    format='%(asctime)s: %(levelname)s:: %(message)s', 
    level=logging.INFO
    )
logging.getLogger("Flask").setLevel(logging.ERROR)

import warnings
warnings.filterwarnings('ignore')

from src.inference.make_scoring_data import main
from src.data.dbutils import push_pandas_df_to_gbq

app = Flask(__name__)

@app.route("/predict", methods=['POST'])
def do_prediction():

    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        data = json.loads(request.data)
        logging.info(f"""{data}""")
        
        try:
            dealID = int(data['dealId'])
        except Exception as e:
            logging.exception(e)
            logging.error("invalid deal_id is supplied")
            return "Invalid DealID supplied!!"
        
        #getting scoring_data
        try:
            scoring_data= main(dealID)
        except:
            return "error in predicting from the model"
        
        #returning for proper deal role
        if not (isinstance(scoring_data,str)):
            req_cols = ['Deal ID','Type of Marketer','Email','pred_prob','Rank']
            rename_columns= {
                "Deal ID":"deal_id",
                "Type of Marketer":"type_of_marketer",
                "Email":"email",
                'pred_prob':'pred_prob',
                'Rank':"rank"
            }
            scoring_data= scoring_data[req_cols]
            scoring_data= scoring_data.rename(rename_columns,axis=1)
        else:
            message= {"message":"No Role found for the supplied dealId"}
            logging.error(f"""{message}""")
            return message
        
        try:
            top_n = int(data['top_n'])
        except Exception as e:
            logging.exception(e)
            result_json= scoring_data.to_json(orient='records')
            scoring_data['created_at']= datetime.now()
            #appending scoring data to google biq query
            push_pandas_df_to_gbq(scoring_data, "sb_ai","scoring_data")
            return result_json
        
        #top_n response
        scoring_df_top_n= scoring_data.iloc[:top_n]
        result_json = scoring_df_top_n.to_json(orient="records")
        #creating time stamp
        scoring_data['created_at']= datetime.now()
        #appending scoring data to google biq query
        push_pandas_df_to_gbq(scoring_data, "sb_ai","scoring_data")
        return result_json
    else:
        logging.error("Content-Type not supported!")
        return 'Content-Type not supported!'
    
if __name__ == "__main__":
    app.run(host="0.0.0.0",port=9080)
