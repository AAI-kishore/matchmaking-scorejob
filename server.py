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

from src.likelyhood_model.make_scoring_data_likely_to_respond import main_likelyhood_model
from src.keyword_search_model.make_scoring_data_keyword_search import main_keyword_search
from src.data.dbutils import push_pandas_df_to_gbq

app = Flask(__name__)

@app.route("/predict", methods=['POST'])
def do_prediction_likelyhood_model():

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
            scoring_data= main_likelyhood_model(dealID)
        except:
            return "error in predicting from the model"
        
        #returning for proper deal role
        if (isinstance(scoring_data,str)):
            message= {"message":"Deal ID or Role is not found"}
            logging.error(f"""{message}""")
            return message
        
        try:
            top_n = int(data['top_n'])
        except Exception as e:
            logging.exception(e)
            result_json= scoring_data.drop('input_epoch_time',axis=1).to_json(orient='records')
            scoring_data['created_at']= datetime.now()
            #appending scoring data to google biq query
            push_pandas_df_to_gbq(scoring_data, "sb_ai","scoring_data")
            return result_json
        
        #top_n response
        scoring_df_top_n= scoring_data.iloc[:top_n]
        result_json = scoring_df_top_n.drop('input_epoch_time',axis=1).to_json(orient="records")
        #creating time stamp
        scoring_data['created_at']= datetime.now()
        #appending scoring data to google biq query
        push_pandas_df_to_gbq(scoring_data, "sb_ai","scoring_data")
        return result_json
    else:
        logging.error("Content-Type not supported!")
        return 'Content-Type not supported!'

@app.route("/keyword_search", methods=['POST'])
def do_prediction_keyword_search():

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
            scoring_data= main_keyword_search(dealID)
        except:
            return "error in predicting from the model"
        
        #returning for proper deal role
        if (isinstance(scoring_data,str)):
            message= {"message":"Deal ID or Role is not found"}
            logging.error(f"""{message}""")
            return message
        
        try:
            top_n = int(data['top_n'])
        except Exception as e:
            logging.exception(e)
            result_json= scoring_data.drop('input_epoch_time',axis=1).to_json(orient='records')
            scoring_data['created_at']= datetime.now()
            #appending scoring data to google biq query
            push_pandas_df_to_gbq(scoring_data, "sb_ai","ai_output_keywordsearch")
            return result_json
        
        #top_n response
        scoring_df_top_n= scoring_data.iloc[:top_n]
        result_json = scoring_df_top_n.drop('input_epoch_time',axis=1).to_json(orient="records")
        #creating time stamp
        scoring_data['created_at']= datetime.now()
        #appending scoring data to google biq query
        push_pandas_df_to_gbq(scoring_data, "sb_ai","ai_output_keywordsearch")
        return result_json
    else:
        logging.error("Content-Type not supported!")
        return 'Content-Type not supported!'
    
if __name__ == "__main__":
    app.run(host="0.0.0.0",port=9080)
