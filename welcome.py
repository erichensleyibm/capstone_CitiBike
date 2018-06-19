# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import requests
import datetime
from flask import Flask, render_template, request
from flask_table import Table, Col
app = Flask(__name__)
VCAP_SERVICES = os.getenv("VCAP_SERVICES")

@app.route('/')
def Welcome():
    # Set global variables
    return render_template('index.html', start_station = VCAP_SERVICES, age = 24)

port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port))
    
from lxml import html
import mysql.connector
import pandas as pd
from sklearn.externals import joblib

global DB_NAME
global PASSWORD
global HOST

DB_NAME = 'citibike'
PASSWORD = 'ibm1234'
HOST = '207.38.142.196'

VCAP_SERVICES = os.getenv("VCAP_SERVICES")
#if VCAP_SERVICES is not None:
    # These will be automatically set if deployed to IBM Cloud
#    SERVICES = json.loads(VCAP_SERVICES)
    # set path when deployed to Bluemix so the same references to other folders can be made as when local
cur_path = '/home/vcap/app'
#else:
    # start with current path
#    cur_path = os.path.abspath(__file__)
#    while cur_path.split('/')[-1] != 'capstone':
#        cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))
      
def conv_time(time_, month_, day_):
    # funtion for scraping time values
    _time = []
    for time in time_:
        hour = int(time.split(' ')[0].split(':')[0])
        minute = int(time.split(' ')[0].split(':')[1])
        tod = time.split(' ')[1]

        hour, minute = str(hour), str(minute)
        if len(hour) == 1:
            hour = '0'+hour
        if len(minute) == 1:
            minute = '0'+minute
        timedate = '2018-%s-%s %s:%s:00' % (month_, day_, hour, minute)
        if tod == 'PM':
            timedate = str(datetime.datetime.strptime(timedate, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours = 12))
        _time.append('"2018-%s-%s %s:%s:00"' % (month_, day_, hour, minute))
    return _time
 
@app.route('/')
def Welcome():
    # Set global variables
    global start_lat
    global start_lon
    global start_station
    global end_station
    global age
    
    # Set GUI defaults
    age = 50
    end_station = 'Destination'
    
    
    # Find latitude and longitude of IP address
    send_url = 'http://freegeoip.net/json'
    r = requests.get(send_url)
    j = json.loads(r.text)
    lat = j['latitude']
    lon = j['longitude']
    
    # Plug latitude and longitude into our DB and query for nearest station.
    cnx = mysql.connector.connect(user='root', password=PASSWORD,
                                      host=HOST,
                                      database=DB_NAME)
    query = 'select station_name, abs(DISTANCE_BETWEEN(%.3f, %.3f, latitude, longitude)), latitude, longitude as `distance` from stations order by abs(DISTANCE_BETWEEN(%.3f, %.3f, latitude, longitude)) limit 1;' % (lat, lon, lat, lon)
    con = cnx.cursor()
    con.execute(query)
    start = con.fetchall()[0]
    start_station = start[0]
    start_lat = start[2]
    start_lon = start[3]
    cnx.close()
    return render_template('index.html', test = VCAP_SERVICES, start_station = '"'+start_station+'"', age = age, end_station = '"'+end_station+'"')

@app.route('/pred_time', methods=['GET', 'POST'])
def pred_time():
    # Retrieve users' input data
    end_station = request.form['end_station']
    gender = request.form['gender']
    age = request.form['ageOutputName']
    user = request.form['user type']
    start_station = request.form['start_station']
    # Cast to integers
    gender = int(gender)
    age = int(age)
    user = int(user)
    
    # Find latitude and longitude of target destination from DB.
    cnx = mysql.connector.connect(user='root', password=PASSWORD,
                                      host=HOST,
                                      database=DB_NAME)
    query = 'select latitude, longitude from stations where station_name = "%s";' % (end_station)
    con = cnx.cursor()
    con.execute(query)
    end = con.fetchall()[0]
    end_lat = end[0]
    end_lon = end[1]

    # Using target destination and current location, calculate distance in miles
    query = 'select DISTANCE_BETWEEN(%.3f, %.3f, %.3f, %.3f);' % (start_lat, start_lon, end_lat, end_lon)
    con = cnx.cursor()
    con.execute(query)
    distance = con.fetchall()[0][0]
    cnx.close()

    # Find current weather from weather underground
    month = datetime.datetime.now().month
    day = datetime.datetime.now().day
    url = 'https://www.wunderground.com/history/airport/KNYC/2018/%s/%s/DailyHistory.html?&reqdb.zip=&reqdb.magic=&reqdb.wmo=' % (month, day)
            
    page = requests.get(url)
    tree = html.fromstring(page.content)
    
    # Reload if necessary due to high use
    while str(page.content)[str(page.content).find('<title>') + 7 : str(page.content).find('</title>')] == "Oops! There\\'s been an error. | Weather Underground":
        print('Error loading page.  Reloading...')
        page = requests.get(url)
        tree = html.fromstring(page.content)
        tree.xpath('//meta[@name="description"]/text()')
    
    month, day = str(month), str(day)
    if len(month) == 1:
        month = '0'+month    
    if len(day) == 1:
        day = '0'+day   
     
    # Set structure using predictable placement of table anchors
    headers = tree.xpath('//div[@id="observations_details"]/table/thead/tr/th/text()')
    headers = ['fill'] + headers
    headers = [i.strip().lower().replace('.','').replace(',', '') for i in headers]
    if 'time' in headers:
        time = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('time')))
    elif 'time (est)' in headers:
         time = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('time (est)')))           
    
    # Send time to function for cleaning
    time = conv_time(time, month, day)
    
    # Split temperature between null and valid entries, back out the full column
    temp_val = tree.xpath('//tr[@class="no-metars"]/td[%i]/span/span[1]/text()' % (headers.index('temp')))
    temp = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('temp')))
    temp = [i.replace('\n','') for i in temp]
    temp = [i for i in temp if i != '']
    temp = [i.replace(' ','') for i in temp]
    for i in range(len(temp)):
        if temp[i] == '':
            temp[i] = temp_val[0]
            temp_val = temp_val[1:]
        else:
            temp[i] = 'NULL'
        
    hum = tree.xpath('//tr[@class="no-metars"]/td[%s]/text()' % (headers.index('humidity')))
    hum = [i.replace('%', '') for i in hum]
    hum = ['NULL' if i == 'N/A' else i for i in hum]
    
    # Split wind between null and valid entries, back out the full column    
    wind = tree.xpath('//tr[@class="no-metars"]/td[%s]/span/span[1]/text()' % (headers.index('wind speed')))
    wind_str = tree.xpath('//tr[@class="no-metars"]/td[%s]/text()' % (headers.index('wind speed')))
    wind_str = [i.replace('\n','') for i in wind_str]
    wind_str = [i for i in wind_str if i != '']
    wind_str = [0 if i == 'Calm' else i for i in wind_str]
    wind_str = [str(i).replace(' ','') for i in wind_str]
    wind_str = [0 if i == '-' else i for i in wind_str]
    if len(wind) > 0:
        for i in range(len(wind_str)):
            if wind_str[i] == '':
                wind_str[i] = wind[0]
                wind = wind[1:]
 
    # Split precipitation between null and valid entries, back out the full column
    prec = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('precip')))
    prec_pres = tree.xpath('//tr[@class="no-metars"]/td[%i]/span/span[1]/text()' % (headers.index('precip')))
    prec = [i for i in prec if i != '\n']
    if len(prec_pres) > 0:
        for i in range(len(prec)):
            if prec[i] == 'N/A':
                prec[i] = '0'
            else:
                prec[i] = prec_pres[0]
                prec_pres = prec_pres[1:]
    prec = [0 if i == 'N/A' else i for i in prec]            
    event = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('events')))
    event = [i.replace('\n', '') for i in event]
    event = ['"None"' if i == '\t\xa0' else '"'+str(i)+'"' for i in event]
    cond = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('conditions')))
    cond = ['"'+str(i)+'"' for i in cond]
        
    pred_date = datetime.datetime.strptime(time[-1][1:-1], "%Y-%m-%d %H:%M:%S")
    
    # Set column structure for test data, as the model is already trained on arbitrarily ordered columns
    data = pd.DataFrame(0, index=range(1), columns = ['temperature','humidity',
         'wind','precip','distance','day_1','day_2','day_3','day_4','day_5','day_6',
         'day_7','month_1','month_2','month_3','month_4','month_5','month_6',
         'month_7','month_8','month_9','month_10','month_11','month_12','hour_0',
         'hour_1','hour_2','hour_3','hour_4','hour_5','hour_6','hour_7','hour_8',
         'hour_9','hour_10','hour_11','hour_12','hour_13','hour_14','hour_15',
         'hour_16','hour_17','hour_18','hour_19','hour_20','hour_21','hour_22',
         'hour_23','user_0','user_1','age','agesqr','gender_0','gender_1','gender_2',
         'condition_Clear','condition_Haze','condition_Heavy Rain','condition_Light Rain',
         'condition_Light Snow','condition_Mostly Cloudy','condition_Overcast',
         'condition_Partly Cloudy','condition_Rain','condition_Scattered Clouds',
         'condition_Unknown','label_Fog\t,Rain','label_None','label_Rain','label_Snow'])
    
    # Fill dataframe with our test values
    data['temperature'] = temp[-1]
    data['humidity'] = hum[-1]
    data['wind'] = wind_str[-1]
    data['precip'] = prec[-1]
    data['distance'] = distance
    data['age'] = age
    data['agesqr'] = age ** 2
    data['label_%s' % (event[-1].replace('"',''))] = 1
    data['condition_%s' % (cond[-1].replace('"',''))] = 1
    data['gender_%s' % (gender)] = 1
    data['user_%s' % (user)] = 1
    use_hour = str(pred_date.hour)
    if len(use_hour) == 1:
        use_hour = '0'+use_hour
    data['month_%s' % (pred_date.month)] = 1
    data['day_%s' % (pred_date.weekday()+1)] = 1
    data['hour_%s' % (pred_date.hour)] = 1

    # Capture weather data for GUI display
    _weather = [{'Aspect':'Wind', 'Measure':wind_str[-1]}, {'Aspect':'Precipitation', 'Measure':prec[-1]}, {'Aspect':'Temperature','Measure':temp[-1]}, {'Aspect':'Humidity','Measure':hum[-1]}, {'Aspect':'Distance (miles)','Measure':'%.3f'%(distance)}]
    # Use Flask_Table to generate HTML
    weather_info = WeatherTable(_weather)
    
    # Load previously trained Linear Regression Model
    model = joblib.load(os.path.join(cur_path, 'models', 'citi_bike_pred.pkl'))
    
    # Divide time by 60 to convert to minutes
    pred_travel = model.predict(data)[0]/60
    return render_template('index_2.html', predict = '%.2f' % (pred_travel), start_station = '"'+start_station+'"', age = age, end_station = '"'+end_station+'"', weather_info = weather_info)

class WeatherTable(Table):
    # set class id and table values
    table_id = 'weather'
    Aspect = Col('Aspect')
    Measure = Col('Measure')      

    
port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port))
