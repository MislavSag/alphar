import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json


# GLOBALS
IP = '206.81.24.140'


# DPSEG
price = np.random.rand(600)
time = np.arange(datetime(2018,1,1), datetime(2019,8,24), timedelta(days=1)).astype(datetime)
t = [t_.timestamp() for t_ in time]
# pd.DataFrame(np.c_[np.array(t), price]).plot()
# with p
x = {
    'time': t,
    'price': price.tolist(),
    'type_': 'var'
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/dpseg", data=x)
res_json = res.json()
print(f'DPSEG without p: , {res_json}')
# without p
x = {
    'time': t,
    'price': price.tolist(),
    'type_': 'var',
    'p': 0.5
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/dpseg", data=x)
res_json = res.json()
print(f'DPSEG with p: , {res_json}')


# BACKCUSUM VOLATILITY
datetime_ = np.arange(datetime(2018,1,1), datetime(2018,3,1), timedelta(minutes=5))  # t least 50 days
datetime_string = np.datetime_as_string(datetime_)
price = np.random.rand(len(datetime_string))
print(len(price))
print(len(datetime_string))
print(price)
print(datetime_string)
x = {
    'time': datetime_string.tolist(),
    'price': price.tolist(),
    'critical_value': 0.4,
    'method_': 'detPer',
    'k_time': 10,
    'marketOpen_': '09:30:00',
    'marketClose_': '16:00:00',
    'tz_': 'America/New_York'
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/backcusumvol", data=x)
res_json = res.json()
print(f'Backcusum volatility: , {res_json[0]}')


# BACKCUSUM
x = {
    'x': price.tolist(),    'rejection_value_down': 0.5,
    'rejection_value_ip': 2.0

}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/backcusum", data=x)
res_json = res.json()
print(f'BackCUSUM , {res_json[0]}')


# Finish
print('Finish!')
