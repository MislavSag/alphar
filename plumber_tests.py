import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json


# GLOBALS
IP = '207.154.227.4'


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


# EXUBER
price = [332.02, 331.72, 331.66, 331.525, 331.55, 331.47, 331.69, 330.9, 329.97, 330.08, 329.89, 329.835, 329.69, 329.68, 328.17, 328.255, 327.93, 328.16, 328.64, 328.89, 328.6, 327.45, 327.6, 327.61, 328.29, 327.97, 327.55, 327.14, 327.96, 327.55, 327.23, 327.31, 327.27, 326.855,
326.65, 325.69, 326.29, 326.765, 326.5, 326.57, 327.2, 327.17, 326.62, 326.5, 326.19, 326.04, 326.53, 326.46, 326.01, 324.42, 325.26, 324.61, 324.89, 324.81, 323.295, 323.71, 322.76, 322.99, 323.15, 322.9, 322.845, 323.25, 322.58, 323.67, 323.0, 322.75, 322.39, 322.28, 322.21, 321.6, 322.44, 323.25, 322.94, 323.41, 323.135, 322.5, 322.745, 324.84, 323.78, 323.165, 323.36, 323.05, 323.25, 323.87, 321.89, 320.94, 320.92, 320.67, 320.61, 320.42, 321.26, 321.1, 321.15, 321.73, 321.83, 321.47, 321.21, 321.47, 322.86, 323.24, 322.92, 323.25, 323.28, 323.2, 322.55, 322.91, 322.2, 322.34, 322.2, 322.29, 322.29, 321.99, 321.26, 321.15, 321.15, 321.04, 321.23, 321.44, 321.39, 321.38, 321.51, 321.58, 321.31, 320.95, 321.36, 321.09, 321.13, 321.15, 320.9, 320.59, 320.91, 320.86, 320.79, 320.54, 320.51, 320.13, 319.95, 319.57, 320.04, 320.01, 319.83, 319.81, 320.0, 319.72, 319.56, 319.84, 319.88, 320.02, 319.91, 319.71, 319.58, 319.51, 319.69, 320.05, 319.98, 320.01, 319.88, 319.38, 317.28, 317.3, 317.43, 317.49, 317.12, 316.55, 317.31, 317.14, 316.71, 316.2, 316.0, 316.16, 317.34, 316.85, 314.41, 314.63, 313.87, 313.79, 313.74, 313.87, 314.41, 313.55, 313.91, 313.84, 313.94, 314.0, 314.31, 313.5, 313.87, 314.3, 314.44, 314.52, 314.6, 315.02, 314.95, 314.83, 314.95, 315.07, 315.22, 315.11, 314.92, 314.63, 312.04, 311.58, 311.57, 311.25, 311.29, 311.36, 311.45, 311.47, 312.03, 311.68, 311.68, 311.58, 311.8, 311.67, 309.48, 308.69, 308.8, 308.45, 308.21, 307.99, 308.17, 311.6, 312.39, 312.04, 311.46, 311.97, 311.49, 313.09, 314.27, 314.75, 314.87, 314.99, 315.44, 315.34, 315.3, 314.94, 314.85, 314.68, 314.61, 314.01, 313.8, 313.67, 314.03, 313.92, 313.7, 313.74, 313.35, 312.94, 312.91, 312.91, 312.92, 312.89, 312.72, 310.98, 310.9, 310.9, 310.85, 310.54, 310.41, 311.1, 310.29, 310.7, 310.38, 310.5, 309.84, 310.07, 310.27, 310.79, 310.38, 309.99, 310.17, 311.36, 311.65, 311.5, 311.89, 312.04, 311.94, 311.91, 312.14, 311.36, 312.05, 312.0, 312.14, 312.14, 311.89, 311.94, 311.63, 311.45, 311.83, 311.49, 311.15, 311.25, 311.25, 310.89, 310.33, 309.53, 309.28, 308.74, 309.03, 308.4, 308.86, 309.2, 309.08, 309.15, 308.83, 309.35, 309.12, 309.0, 308.37, 308.94, 308.4, 309.51, 309.32, 309.38, 309.76, 309.43, 308.39, 308.39, 308.51, 308.32, 308.11, 307.91, 308.01, 308.96, 308.22, 308.1, 308.31, 308.26, 308.11, 307.32, 308.22, 308.39, 308.76, 309.09, 309.38, 309.21, 309.04, 307.13, 307.05, 307.1, 306.48, 306.36, 306.78, 306.96, 307.0, 307.38, 307.32, 307.3, 307.01, 306.91, 307.38, 307.34, 307.42, 307.59, 307.51, 307.36, 307.53, 307.63, 306.19, 305.79, 305.85, 305.28, 305.53, 305.62, 305.23, 303.26, 302.32, 302.37, 302.2, 302.68, 302.8, 303.28, 304.14, 303.67, 302.5, 302.9, 302.79, 303.06, 302.87, 303.2, 303.11, 303.79, 303.82, 303.27, 303.98, 303.33, 303.36, 303.51, 303.44, 303.19, 303.28, 303.34, 303.5, 301.64, 301.67, 301.73, 301.76, 302.0, 301.34, 300.37, 300.42, 300.15, 299.75, 300.12, 300.56, 299.92, 299.88, 299.86, 299.06, 299.29, 299.34, 299.44, 299.45, 299.28, 299.01, 299.68, 300.5, 300.76, 300.29, 300.66, 300.67, 300.04, 299.96, 299.69, 299.84, 299.63, 299.58, 299.17, 297.98, 298.62, 298.57, 297.85, 297.98, 298.63, 299.31, 299.24, 299.58, 299.38, 299.3, 298.91, 299.25, 300.09, 298.41, 298.56, 298.1, 298.22, 298.66, 299.0, 298.16, 298.9, 299.29, 299.49, 299.37, 299.22, 299.45, 297.43, 296.01, 296.22, 296.35, 296.15, 296.33, 296.59, 296.14, 296.27, 298.3, 298.13, 297.65, 297.76, 297.71, 297.28, 293.25, 292.84, 293.4, 293.27, 293.99, 293.79, 293.18, 291.23, 291.94, 291.38, 291.33, 290.94, 290.75, 290.43, 288.55, 290.48, 290.62, 290.34, 289.55, 290.16, 289.66, 293.11, 293.74, 295.15, 294.35, 294.83, 294.07, 293.16, 294.37, 293.57, 293.03, 292.91, 292.9, 291.85, 292.29, 290.38, 289.71, 289.98, 289.47, 288.09, 287.4, 288.33, 288.06, 288.48, 287.31, 286.97, 288.22, 288.39, 289.78, 293.22, 293.84, 293.24, 294.07, 294.73, 295.4, 298.19, 296.83, 297.07, 297.3, 297.36, 297.03, 296.57, 296.5, 295.35, 293.98, 295.23, 294.91, 296.64, 297.6, 296.89, 296.97, 296.96, 296.88, 296.59, 295.86, 296.56, 296.74, 297.6, 297.5, 296.9, 296.47, 296.43, 296.26, 295.19, 295.81, 295.78, 295.47, 295.25, 297.5, 298.89, 299.59, 298.25, 298.6, 298.72, 298.45, 298.44, 297.69, 298.05, 298.13, 298.94, 298.67, 300.01, 300.12, 300.24, 300.27, 301.06, 301.55, 301.77, 301.92, 302.29, 302.18, 301.88, 301.09, 299.06, 300.17, 300.02, 300.18, 300.28, 300.16, 300.94, 300.29, 300.3, 300.0, 300.29, 299.97, 300.16, 300.16, 300.24, 300.26, 299.89]
x = {'x': price, 'adf_lag': 2}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/radf", data=x)
res_json = res.json()
print(f'Exuber: , {res_json["bsadf"]}')


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
print(f'Gpd ES , {res_json}')

# Finish
print('Finish!')

# ML MODEL RISKS
x = {
    'features': np.diff(np.log(price[:10])).tolist(),
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/ml_model_risks", data=x)
res_json = res.json()
print(f'ML model probabilities , {res_json}')

# Radf point
url = "http://" + IP + "/alphar/radf_point"
x = {
    "symbols": "AAPL",
    "date": "20210101",
    "window": 100,
    "price_lag": 1,
    "use_log": 1,
    "time": "hour"
}
res = requests.get(url, params = x)
res_json = res.json()
print(f'Radf point , {res_json}')
print(f'Radf point sadf, {res_json[0]["sadf"]}')

# Radf point stock minute
url = "http://" + IP + "/alphar/radf_point"
x = {
    "symbols": "AAPL",
    "date": "20210101",
    "window": 100,
    "price_lag": 1,
    "use_log": 1,
    "time": "minute"
}
res = requests.get(url, params = x)
res_json = res.json()
print(f'Radf point stock minute, {res_json}')
print(f'Radf point sadf stock minute, {res_json[0]["sadf"]}')

# Radf point SP500
url = "http://" + IP + "/alphar/radf_point_sp"
x = {
    "date": "20210101000000",
    "window": 100,
    "price_lag": 1,
    "use_log": 1,
    "agg_type": "std",
    "number_of_assets": 10
}
res = requests.get(url, params = x)
res_json = res.json()
print(f'Radf point SP500, {res_json}')
print(f'Radf point sadf SP500, {res_json[0]["sadf"]}')

# Radf point crypto
url = "http://" + IP + "/alphar/radf_point"
x = {
    "symbols": "BTCUSD",
    "date": "20210101",
    "window": 100,
    "price_lag": 1,
    "use_log": 1,
    "time": "hour"
}
res = requests.get(url, params = x)
res_json = res.json()
print(f'Radf point crypto, {res_json}')
print(f'Radf point sadf crypto, {res_json[0]["sadf"]}')

# Radf point crypto minute
url = "http://" + IP + "/alphar/radf_point"
x = {
    "symbols": "BTCUSD",
    "date": "20210101",
    "window": 100,
    "price_lag": 1,
    "use_log": 1,
    "time": "minute"
}
res = requests.get(url, params = x)
res_json = res.json()
print(f'Radf point crypto minute, {res_json}')
print(f'Radf point sadf crypto minute, {res_json[0]["sadf"]}')

# quark POST
x = {
    "x": price.tolist(),
    "p": 0.975,
    "model": "EWMA",
    "method": "plain",
    "nwin": 100,
    "nout": 150
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/quark", data=x)
res_json = res.json()
print(f'Quark risk factors , {res_json}')

# BachCUSUM filter
price = np.random.rand(600)
x = {
    "returns": price.tolist()
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/backcusumfilter", data=x)
res_json = res.json()
print(f'Backcusum filter , {res_json}')
print(f'Backcusum filter , {res_json[0]["backcusum_rejections_1"]}')

# mlr3 hft model predicitons
x = {
    "close": price.tolist()
}
x = json.dumps(x)
res = requests.post("http://" + IP + "/alphar/ml_model_hft", data=x)
res_json = res.json()
print(f'mlr3 hft model: , {res_json}')
