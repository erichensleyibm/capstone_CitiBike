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

import requests, zipfile, io
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import datetime
from lxml import html

global DB_NAME
DB_NAME = 'citibike'

def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf16'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

def create_tables(cursor):
    TABLES = {}
    TABLES['stations'] = (
        "CREATE TABLE `stations` ("
        "  `station_id` int(5) NOT NULL,"
        "  `station_name` varchar(60) NOT NULL,"    
        "  `latitude` FLOAT(10, 6) NOT NULL,"
        "  `longitude` FLOAT(10, 6) NOT NULL,"
        "  PRIMARY KEY (`station_id`)"
        ") ENGINE=InnoDB")

    TABLES['weather'] = (
        "CREATE TABLE `weather` ("
        "  `time_id` DATETIME NOT NULL UNIQUE,"
        "  `temperature` FLOAT(4,1),"    
        "  `humidity` INT,"
        "  `wind` FLOAT(3,1) NOT NULL,"    
        "  `precip` FLOAT(4,2) NOT NULL,"
        "  `label` VARCHAR(10) NOT NULL,"
        "  `condition` VARCHAR(25) NOT NULL,"
        "  PRIMARY KEY (`time_id`)"
        ") ENGINE=InnoDB")
    
    TABLES['trips'] = (
        "CREATE TABLE `trips` ("
        "  `trip_id` int(11) NOT NULL AUTO_INCREMENT,"
        "  `start` DATETIME NOT NULL,"    
        "  `end` DATETIME NOT NULL,"    
        "  `avg_trip` DATETIME NOT NULL,"
        "  `duration` INT NOT NULL,"
        "  `start_station` int(5) NOT NULL,"
        "  `end_station` int(5) NOT NULL,"
        "  `bike_id` int(5) NOT NULL,"
        "  `user` TINYINT NOT NULL,"
        "  `gender` TINYINT NOT NULL,"
        "  `birth_year` INT,"
        "  PRIMARY KEY (`trip_id`),"
        "  CONSTRAINT `fk_start_station` FOREIGN KEY (`start_station`) "
        "     REFERENCES `stations` (`station_id`) ON DELETE CASCADE "
        "     ON UPDATE CASCADE, "
        "  CONSTRAINT `fk_end_station` FOREIGN KEY (`end_station`) "
        "     REFERENCES `stations` (`station_id`) ON DELETE CASCADE "
        "     ON UPDATE CASCADE"
        ") ENGINE=InnoDB")
    
    for name, ddl in TABLES.items():
        try:
            print("Creating table {}: ".format(name), end='')
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")    
    
def set_mysql_env(cnx):
    cursor = cnx.cursor()
    try:
        cnx.database = DB_NAME
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)             
    create_tables(cursor)
    cursor.close()

def get_data(month, cache = False):    
    month = str(month)
    if len(month) == 1:
        month = '0'+month
    if '2017%s-citibike-tripdata.csv' % (month) in os.listdir(os.path.join(cur_path, 'data')):
        month_data = pd.read_csv(os.path.join(cur_path, 'data', '2017%s-citibike-tripdata.csv' % (month)))
    else:
        zip_file_url = "https://s3.amazonaws.com/tripdata/2017%s-citibike-tripdata.csv.zip" % (month)
        r = None
        r = requests.get(zip_file_url)
        while not r.ok:
            r = requests.get(zip_file_url)     
        z = zipfile.ZipFile(io.BytesIO(r.content))
        if cache:
            z.extractall(os.path.join(cur_path, 'data'))
        month_data = pd.read_csv(z.open('2017%s-citibike-tripdata.csv' % (month)))
    return month_data

def trip_insert(cnx, month_data, batch_size):
    month_insert = month_data[['start time', 'stop time', 'trip_time', 'trip duration', 'start_station', 'end_station',
                               'bike id', 'usertype', 'gender', 'birth year']].fillna('NULL').values
    base = 'INSERT INTO trips (start, end, avg_trip, duration, start_station, end_station, bike_id, user, gender, birth_year) VALUES '
    running_insert = 0
    
    while running_insert != len(month_insert):
        insert_vals = []
        while len(insert_vals) < batch_size and running_insert != len(month_insert):
            insert_vals.append('("%s", "%s", %s, %s, %s, %s, %s, %s, %s, %s)' %
                               (*month_insert[running_insert],))
            running_insert += 1
        full_insert = base+','.join(insert_vals)+';'
        cnx.cursor().execute('SET foreign_key_checks = 0;')
        cnx.cursor().execute(full_insert)
        cnx.commit()
        cnx.cursor().execute('SET foreign_key_checks = 1;')

def station_insert(cnx, station_dict):
    base = 'INSERT INTO stations VALUES '
    insert_vals = []
    for (name, data) in station_dict.items():
        insert_vals.append('(%s, "%s", %s, %s)' % (data['id'], name, data['latitude'], data['longitude']))
    full_insert = base+','.join(insert_vals)+';'
    cnx.cursor().execute('SET foreign_key_checks = 0;')
    cnx.cursor().execute(full_insert)
    cnx.commit()
    cnx.cursor().execute('SET foreign_key_checks = 1;')    

def conv_time(time_, month_, day_):
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
        timedate = '2017-%s-%s %s:%s:00' % (month_, day_, hour, minute)
        if tod == 'PM':
            timedate = str(datetime.datetime.strptime(timedate, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours = 12))
        _time.append('"2017-%s-%s %s:%s:00"' % (month_, day_, hour, minute))
    return _time
    
def weather_insert(cnx, weather_data):
    weather_data = weather_data.fillna('NULL').values
    base = 'INSERT INTO weather VALUES '
    
    insert_vals = []
    for running_insert in range(len(weather_data)):
        insert_vals.append('(%s, %s, %s, %s, %s, %s, %s)' %
                                   (*weather_data[running_insert],))
    full_insert = base+','.join(insert_vals)+';'
    cnx.cursor().execute('SET foreign_key_checks = 0;')
    cnx.cursor().execute(full_insert)
    cnx.commit()
    cnx.cursor().execute('SET foreign_key_checks = 1;')

def pull_weather(cnx):
    print('Beginning weather data retrieval and storage...')

    con = cnx.cursor()
    con.execute("Select Max(time_id) from weather;")
    latest = con.fetchall()
    
    cur_date = datetime.datetime(latest[0][0].year, latest[0][0].month, latest[0][0].day +  1)

#    cur_date = datetime.datetime(2017, 1, 1)         
    while cur_date.year == 2017:
        print('Weather Date: %s' % (cur_date.date()))
        month = cur_date.month
        day = cur_date.day
        url = 'https://www.wunderground.com/history/airport/KNYC/2017/%s/%s/DailyHistory.html?&reqdb.zip=&reqdb.magic=&reqdb.wmo=' % (month, day)
        
        page = requests.get(url)
        tree = html.fromstring(page.content)
        tree.xpath('//meta[@name="description"]/text()')
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
            
        headers = tree.xpath('//div[@id="observations_details"]/table/thead/tr/th/text()')
        headers = ['fill'] + headers
        headers = [i.strip().lower().replace('.','').replace(',', '') for i in headers]
        if 'time' in headers:
            time = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('time')))
        elif 'time (est)' in headers:
             time = tree.xpath('//tr[@class="no-metars"]/td[%i]/text()' % (headers.index('time (est)')))           
        time = conv_time(time, month, day)
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
        
        weatherdata = pd.DataFrame()
        weatherdata['time_id'] = time
        weatherdata['temperature'] = temp
        weatherdata['humidity'] = hum
        weatherdata['wind'] = wind_str
        weatherdata['precip'] = prec
        weatherdata['label'] = event
        weatherdata['condition'] = cond
        
        weatherdata.drop_duplicates(subset = 'time_id', inplace = True)
        weather_insert(cnx, weatherdata)
        cur_date = cur_date + datetime.timedelta(hours = 24)
        
def store_data(cnx, batch_size = 10000):
    con = cnx.cursor()
    con.execute("Select Max(trip_id) from trips;")
    full_check = con.fetchall()
    if full_check[0][0] == 16364657:
        return
    
    station_dict = {}
    
    running_id = 0
    for month in range(1,13):    
        month_data = get_data(month)
        print('Processing data for %s/2017' % (month))
        month_data.columns = [x.lower() for x in month_data.columns]
        for colname in ['trip duration', 'start time', 'stop time', 'user type', 'bike id']:
            if ''.join(colname.split(' ')) in list(month_data):
                month_data.rename(columns = {''.join(colname.split(' ')): colname}, inplace = True)
        for name, lat, lon in month_data[['start station name', 'start station latitude', 'start station longitude']].values:
            if name not in station_dict.keys():
                station_dict[name] = {'id': running_id, 'latitude': lat, 'longitude': lon}
                running_id += 1
        for name, lat, lon in month_data[['end station name', 'end station latitude', 'end station longitude']].values:
            if name not in station_dict.keys():
                station_dict[name] = {'id': running_id, 'latitude': lat, 'longitude': lon}
                running_id += 1
        month_data['start_station'] = month_data['start station name'].apply(lambda x: station_dict[x]['id'])
        month_data['end_station'] = month_data['end station name'].apply(lambda x: station_dict[x]['id'])
        month_data['usertype'] = month_data['user type'].apply(lambda x: 0 if x == 'Customer' else 1)
        
        stop = month_data['stop time'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        start = month_data['start time'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        avg_trip = [j + (i - j) for i,j in zip(stop, start)]
        avg_trip = ['"'+str(i)+'"' for i in avg_trip]
        month_data['trip_time'] = avg_trip
        print('Storing data for %s/2017' % (month))
        trip_insert(cnx, month_data, batch_size)
    station_insert(cnx, station_dict)

if __name__ == '__main__':
    cnx = mysql.connector.connect(user='root', password = 'ibm1234')
    set_mysql_env(cnx)
    cnx.close()
    cnx = mysql.connector.connect(user='root', password='ibm1234',
                                  host='127.0.0.1',
                                  database=DB_NAME)
    store_data(cnx = cnx)
    pull_weather(cnx)   
    cnx.close()
