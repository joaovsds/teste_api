import pickle
import pandas as pd
from flask             import Flask, request, Response
from rossmann.Rossmann import Rossmann

#loading model
model = pickle.load(open('/home/jocker/gold/projetos/ds_prod/dsemproducao/model/model_rossmann.pkl','rb'))

# initializate
app = Flask(__name__)

@app.route('/rossmann/predict', methods=['POST'])
def rossmann_predict():
    test_json = request.get_json()
    
    if test_json: # there is data
        if isinstance( test_json, dict): #unique example
            test_raw = pd.DataFrame( test_json, index[0])
        
        else: # multiple example
            test_raw = pd.DataFrame( test_json, columns=test_json[0].keys())
            
        # instantiate Rossman class
        pipeline = Rossmann()
        
        #data cleaning
        df1 = pipeline.data_cleaning( test_raw)
        
        # feature engineering
        df2 = pipeline.feature_engineering( df1)
        
        # data preparation
        df3 = pipeline.data_preparation(df2)
        
        # prediction
        df_response = pipeline.get_prediction(model, test_raw, df3)
        
        return df_response
    
    else: 
        return Response( '{}', status=200, mimetype='application/json')
        
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)