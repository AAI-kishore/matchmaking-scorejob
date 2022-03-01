from flask import Flask, request
import pandas as pd
import joblib
import xgboost as xgb
import json

import logging
logging.basicConfig(
    format='%(asctime)s: %(levelname)s:: %(message)s', 
    level=logging.INFO
    )
logging.getLogger("Flask").setLevel(logging.ERROR)

import warnings
warnings.filterwarnings('ignore')

from src.inference.make_scoring_data import main

app = Flask(__name__)

@app.route("/predict", methods=['POST'])
def do_prediction():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        data = json.loads(request.data)
        logging.info(f"""{data}""")
        try:
            dealID = int(data['dealId'])
            topn_n = int(data['top_n'])
            result_json = getPredictions(dealID,topn_n)
            return result_json
        except Exception as e:
            logging.exception(e)
            logging.error("invalid deal_id is supplied")
            return "Invalid DealID supplied!!"
    else:
        logging.error("Content-Type not supported!")
        return 'Content-Type not supported!'
   
def getPredictions(dealID,n):
    scoring_data= main(dealID)
    if not (isinstance(scoring_data,str)):
        req_cols = ['Deal ID','Type of Marketer','Email','pred_prob','Rank']
        top_n_results = scoring_data[req_cols].head(n)
        rename_cols = {"Deal ID":"deal_Id","Type of Marketer":"role","Email":"email","Rank":"rank"}
        top_n_results = top_n_results.rename(columns=rename_cols)
        result_json = top_n_results.to_json(orient='records')
        return result_json
    else:
        message= {"message":"No Role found for the supplied dealId"}
        logging.error(f"""{message}""")
        return message
    
if __name__ == "__main__":
    app.run(host="0.0.0.0",port=9080)
