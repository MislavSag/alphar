import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json


# DPSEG
price = np.random.rand(600)
t = np.arange(datetime(2018,1,1), datetime(2019,8,24), timedelta(days=1)).astype(datetime)
t = [t_.timestamp() for t_ in t]
pd.DataFrame(np.c_[np.array(t), price]).plot()
# with p
x = {
    'time': t,
    'price': price.tolist(),
    'type_': 'var'
}
x = json.dumps(x)
res = requests.post("http://46.101.219.193/alphar/dpseg", data=x)
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
res = requests.post("http://46.101.219.193/alphar/dpseg", data=x)
res_json = res.json()
print(f'DPSEG with p: , {res_json}')

# Finish
print('Finish!')
