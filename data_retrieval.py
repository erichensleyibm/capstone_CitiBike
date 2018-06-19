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

import pandas as pd
import mysql.connector

global DB_NAME
DB_NAME = 'citibike'

def pull_data():
    query = "SELECT\
        DAYOFWEEK(start) as `day`,\
        hour(start) as `hour`,\
        month(start) as `month`,\
        duration,\
        start_station,\
        end_station,\
        bike_id,\
        user,\
        gender,\
        birth_year,\
        temperature,\
        humidity,\
        wind,\
        precip,\
        label,\
        `condition`,\
        DISTANCE_BETWEEN(s1.`latitude`,\
                s1.`longitude`,\
                s2.`latitude`,\
                s2.`longitude`) AS `distance`\
        FROM\
        trips AS t\
            JOIN\
        weather AS w1 ON w1.time_id = (SELECT\
                time_id\
            FROM\
                weather AS w\
            WHERE\
                DAY(w.time_id) = DAY(t.start)\
            ORDER BY ABS(TIMESTAMPDIFF(SECOND,\
                        w.time_id,\
                        t.start)) ASC\
            LIMIT 1) \
    JOIN \
        stations AS s1 \
        on t.start_station = s1.station_id \
    JOIN \
    	stations as s2 \
        on t.end_station = s2.station_id;"
        
    cnx = mysql.connector.connect(user='root', password='ibm1234',
                                  host='127.0.0.1',
                                  database=DB_NAME)
    
    con = cnx.cursor()
    con.execute(query)
    full_data = con.fetchall()  
    
    full_data = pd.DataFrame(full_data, columns = ['day', 'hour', 'month', 'duration', 'start_station',
                                                   'end_station', 'bike_id', 'user', 'gender', 'birthyear',
                                                   'temperature', 'humidity', 'wind', 'precip', 'label',
                                                   'condition', 'distance'])
        
    full_data['mph'] = full_data['distance'] / (full_data['duration'] / 3600)
    full_data.to_csv(os.path.join(cur_path, 'data', 'full_citibike_data.csv'))

if __name__ == '__main__':
    pull_data()