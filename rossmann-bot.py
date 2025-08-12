import json
import pandas as pd
import requests
from flask import Flask, request, Response

#constants
TOKEN = '8438363273:AAG0cTTY8d8IG39A_MIv0WXgWTS65gg8Hb4'

# # info bot
# https://api.telegram.org/bot8438363273:AAG0cTTY8d8IG39A_MIv0WXgWTS65gg8Hb4/getMe

# # GetUpdates
# https://api.telegram.org/bot8438363273:AAG0cTTY8d8IG39A_MIv0WXgWTS65gg8Hb4/getUpdates

# # Webhook
# https://api.telegram.org/bot8438363273:AAG0cTTY8d8IG39A_MIv0WXgWTS65gg8Hb4/setWebhook?url=https://09b2ef8961a98d.lhr.life

# # send message
# https://api.telegram.org/bot8438363273:AAG0cTTY8d8IG39A_MIv0WXgWTS65gg8Hb4/sendMessage?chat_id=764302199&text=testando mensagem...

def send_message(chat_id,text):
    url = 'https://api.telegram.org/bot{}/'.format(TOKEN)
    url = url + 'sendMessage?chat_id={}'.format(chat_id)
    
    r = requests.post(url, json={'text':text})
    print('Status Code{}'.format(r.status_code))
    
    return None

def load_dataset(store_id):
    # load test dataset
    df10 = pd.read_csv(r'C:\Users\joaog\OneDrive\Documentos\repos\Ds_em_producao\dsemproducao\data\test.csv') 
    df_store_raw = pd.read_csv(r'C:\Users\joaog\OneDrive\Documentos\repos\Ds_em_producao\dsemproducao\data\store.csv')

    # merge test + store
    df_test = pd.merge( df10, df_store_raw, how = 'left', on = 'Store')

    # choose store for prediction
    df_test = df_test[df_test['Store'] == store_id]
    
    if not df_test.empty:
        # remove closed days and column id
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop('Id', axis=1)

        # convert dataframe to json
        data1 = json.dumps( df_test.to_dict(orient='records'))
        
    else:
        data1 = 'error'
        
    return data1

def predict(data1):
    # API Call
    #url = 'http://127.0.0.1:5000/rossmann/predict'
    url = 'https://teste-api-80xt.onrender.com/rossmann/predict'
    header = {'Content-type': 'application/json'}
    data = data1

    r = requests.post(url, data = data1, headers = header)
    print('Status Code {}'.format(r.status_code))

    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())
    
    return d1

def parse_message( message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']
    
    store_id = store_id.replace('/', '')
    
    try:
        store_id = int(store_id)
    
    except ValueError:
        store_id = 'error'
        
    return chat_id, store_id

# API 
app = Flask( __name__)

@app.route('/', methods=['GET', 'POST'])

def index():
    if request.method == 'POST':
        message = request.get_json()
        
        chat_id, store_id = parse_message(message)
        
        if store_id != 'error':
            #loadind data
            data = load_dataset(store_id)
            
            if data != 'error':
                #prediction
                d1 = predict(data)
         
                #calculation
                d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()
                
                #send message
                msg = 'A Loja número {} vai vender R${:,.2f} nas proxímas 6 semanas'.format(d2['store'].values[0],
                                                                                            d2['prediction'].values[0])
                
                send_message(chat_id, msg)
                return Response( 'ok', status=200)

            
            else:
                send_message(chat_id, 'O Store ID não está disponível ')
                return Response('OK', status=200)

        else:
            send_message(chat_id, 'O Store ID está incorreto')
            return Response('OK', status=200)
    else: 
        return '<h1> Rossmann Telegram BOT <h1>'
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
    
