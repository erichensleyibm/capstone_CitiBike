import os, sys
try:                                            # if running in CLI
    cur_path = os.path.abspath(__file__)
    while cur_path.split('/')[-1] != 'capstone':
        cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))
except NameError:                               # if running in IDE
    cur_path = os.getcwd()
    while cur_path.split('/')[-1] != 'capstone':
        cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))
    sys.path.insert(1, os.path.join(cur_path, 'lib', 'python3.6', 'site-packages'))
sys.path.insert(2, os.path.join(cur_path, 'lib','LightGBM', 'python-package'))
    
import pandas as pd
import numpy as np
import mysql.connector
DB_NAME = 'citibike'

def modeling():
    raw_data = pd.read_csv(os.path.join(cur_path, 'data', 'full_citibike_data.csv'))
    raw_data.drop('Unnamed: 0', axis = 1, inplace = True)
    
    dur_sd_05 = raw_data['duration'].quantile(0.05)
    dur_sd_95 = raw_data['duration'].quantile(0.95)
    
    temp_sd_05 = raw_data['temperature'].quantile(0.05)
    temp_sd_95 = raw_data['temperature'].quantile(0.95)
    
    hum_sd_05 = raw_data['humidity'].quantile(0.05)
    hum_sd_95 = raw_data['humidity'].quantile(0.95)
    
    wind_sd_95 = raw_data['wind'].quantile(0.95)
    
    mph_sd_05 = raw_data['mph'].quantile(0.05)
    mph_sd_95 = raw_data['mph'].quantile(0.95)
    
    raw_data = raw_data.loc[(raw_data['duration'] > dur_sd_05) & (raw_data['duration'] < dur_sd_95)]
    raw_data = raw_data.loc[(raw_data['temperature'] > temp_sd_05) & (raw_data['temperature'] < temp_sd_95)]
    raw_data = raw_data.loc[(raw_data['humidity'] > hum_sd_05) & (raw_data['humidity'] < hum_sd_95)]
    raw_data = raw_data.loc[(raw_data['mph'] > mph_sd_05) & (raw_data['mph'] < mph_sd_95)]
    raw_data = raw_data.loc[raw_data['wind'] < wind_sd_95]
    
    raw_data = pd.concat([raw_data, pd.get_dummies(raw_data['day'], prefix = 'day')], axis=1)
    raw_data = pd.concat([raw_data, pd.get_dummies(raw_data['month'], prefix = 'month')], axis=1)
    raw_data = pd.concat([raw_data, pd.get_dummies(raw_data['hour'], prefix = 'hour')], axis=1)
    raw_data = pd.concat([raw_data, pd.get_dummies(raw_data['user'], prefix = 'user')], axis=1)
    
    raw_data['age'] = 2018 - raw_data['birthyear']
    raw_data['agesqr'] = raw_data['age'] ** 2
    raw_data = pd.concat([raw_data, pd.get_dummies(raw_data['gender'], prefix = 'gender')], axis=1)
    raw_data = pd.concat([raw_data, pd.get_dummies(raw_data['condition'], prefix = 'condition')], axis=1)
    raw_data = pd.concat([raw_data, pd.get_dummies(raw_data['label'], prefix = 'label')], axis=1)
    raw_data = raw_data[[i for i in list(raw_data) if i not in ['day', 'month', 'hour', 'gender', 'birthyear', 'condition', 'label', 'bike_id', 'user', 'mph', 'start_station', 'end_station']]]
    
    raw_data.replace([np.inf, -np.inf], np.nan, inplace = True)
    raw_data.dropna(how = 'any', inplace = True)
    
    raw_data.to_csv(os.path.join(cur_path, 'data', 'citibike_analysis.csv'))

def visualization():
    cnx = mysql.connector.connect(user='root', password='ibm1234',
                                      host='127.0.0.1',
                                      database=DB_NAME)
    
    con = cnx.cursor()
    con.execute("Select station_id, station_name from stations;")
    station_dict = dict(con.fetchall())
    
    raw_data = pd.read_csv(os.path.join(cur_path, 'data', 'full_citibike_data.csv'))
    raw_data.drop('Unnamed: 0', axis = 1, inplace = True)

    raw_data.replace([np.inf, -np.inf], np.nan, inplace = True)
    raw_data.dropna(how = 'any', inplace = True)
    
    dur_sd_05 = raw_data['duration'].quantile(0.05)
    dur_sd_95 = raw_data['duration'].quantile(0.95)
    
    temp_sd_05 = raw_data['temperature'].quantile(0.05)
    temp_sd_95 = raw_data['temperature'].quantile(0.95)
    
    hum_sd_05 = raw_data['humidity'].quantile(0.05)
    hum_sd_95 = raw_data['humidity'].quantile(0.95)
    
    wind_sd_95 = raw_data['wind'].quantile(0.95)
    
    mph_sd_05 = raw_data['mph'].quantile(0.05)
    mph_sd_95 = raw_data['mph'].quantile(0.95)
    
    raw_data = raw_data.loc[(raw_data['duration'] > dur_sd_05) & (raw_data['duration'] < dur_sd_95)]
    raw_data = raw_data.loc[(raw_data['temperature'] > temp_sd_05) & (raw_data['temperature'] < temp_sd_95)]
    raw_data = raw_data.loc[(raw_data['humidity'] > hum_sd_05) & (raw_data['humidity'] < hum_sd_95)]
    raw_data = raw_data.loc[(raw_data['mph'] > mph_sd_05) & (raw_data['mph'] < mph_sd_95)]
    raw_data = raw_data.loc[raw_data['wind'] < wind_sd_95]
    
    raw_data['start_station'] = raw_data['start_station'].apply(lambda x: station_dict[x])
    raw_data['end_station'] = raw_data['end_station'].apply(lambda x: station_dict[x])
    
    raw_data['user'] = raw_data['user'].apply(lambda x: 'Customer' if x == 0 else 'Subscriber')
    raw_data['gender'] = raw_data['gender'].apply(lambda x: 'Unknown' if x == 0 else x)
    raw_data['gender'] = raw_data['gender'].apply(lambda x: 'male' if x == 1 else 'female')
    
    trip = []
    for i,j in raw_data[['start_station', 'end_station']].values:
        trip.append(i+' -> '+j)
    raw_data['trip'] = trip
    raw_data['age'] = 2018 - raw_data['birthyear']

    trip_data = raw_data['trip']
    trip_data.to_csv(os.path.join(cur_path, 'data', 'trip_vis.csv'))
    
    raw_data = raw_data[['age', 'duration', 'start_station', 'end_station', 'bike_id', 'mph', 'user', 'gender', 'distance']]
    raw_data.to_csv(os.path.join(cur_path, 'data', 'citibike_vis.csv'))

