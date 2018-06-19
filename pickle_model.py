import os, sys
try:                                            # if running in CLI
    cur_path = os.path.abspath(__file__)
    while cur_path.split('/')[-1] != 'capstone':
        cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))

except NameError:                               # if running in IDE
    cur_path = os.getcwd()
    while cur_path.split('/')[-1] != 'capstone':
        cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))
    sys.path.insert(1, os.path.join(cur_path, 'capstone', 'lib', 'python3.6', 'site-packages'))
    
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib

def store_lm():
    # Read data
    data = pd.read_csv(os.path.join(cur_path, 'data', 'citibike_analysis.csv'))
    # Drop index column
    data.drop('Unnamed: 0', axis = 1, inplace = True)
    
    # Split between x and y
    x_data = data.loc[:, data.columns != 'duration']
    y_data = data['duration']
    data = None
    
    # Fit model
    model = Pipeline([('scale', StandardScaler()), ('reg', LinearRegression())])
    model.fit(x_data, y = y_data)
    
    # Store Model
    joblib.dump(model, os.path.join(cur_path, 'models', 'citi_bike_pred.pkl'))

if __name__ == '__main__':
    if not os.path.isfile(os.path.join(cur_path, 'models', 'citi_bike_pred.pkl')):
        store_lm()