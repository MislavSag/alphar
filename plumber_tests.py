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
x = {
    'time': t,
    'price': price.tolist(),
    'type_': 'var',
    'p': 0.2
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
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/backcusum", data=x)
res_json = res.json()
print(f'BackCUSUM , {res_json[0]}')


# VaR
x = {
    'x': price.tolist(),
    'prob': 0.99,
    'type': 'modified'
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/varrisk", data=x)
res_json = res.json()
print(f'Var , {res_json[0]}')

# GAS VaR
x = {
    'x': price[:750].tolist(),
    'dist': 'std',
    'scaling_type': 'Identity',
    'h': 1,
    'p': 0.01
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/gas", data=x)
res_json = res.json()
print(f'Var , {res_json[0]}')

# General pareto distribution fit
x = {
    'x': np.diff(np.log(price[:750])).tolist(),
    'threshold':  -0.001,
    'method': 'pwm',
    'p': 0.999
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/gpd", data=x)
res_json = res.json()
print(f'Gpd ES , {res_json[0]}')

# Finish
print('Finish!')
