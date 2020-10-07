import numpy as np
import pandas as pd
import json, requests

#get json data
f = open('orders.json')
q1_orders = json.load(f)
f.close()

#get api exchange rates
response = requests.get('https://api.exchangeratesapi.io/history?start_at=2020-01-01&end_at=2020-03-31&base=USD&symbols=CAD')
response.raise_for_status()
fx = json.loads(response.text)



#create two dataframes join on date
df_q1 = pd.json_normalize(q1_orders)
df_fx = pd.DataFrame(fx)
df_fx['cad_rates'] = df_fx['rates'].apply(lambda x: x['CAD'])
df_q1['order_date'] = df_q1['created_at'].apply(lambda x: x[:10])
df_join_q1_fx = pd.merge(left=df_q1,right=df_fx,left_on='order_date',right_index=True)
df_join_q1_fx['total_price_cad'] = round((df_join_q1_fx['total_price'] * df_join_q1_fx['cad_rates']),2)