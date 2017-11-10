# -*- coding: utf-8 -*-
# This program is used for calculating the rising and setting of Sun...

import ephem                                    #一个天文计算库，用于计算太阳的日出日落时间
import datetime

def calculateSunriseAndSunset(latitude, longitude):             #传入的经纬度必须为浮点型
    observer = ephem.Observer()
    
    lat_degree = int(latitude)
    lat_min = int((latitude - lat_degree)*60)
    lat_sec = (latitude - lat_degree - lat_min/60)*60*60
    observer.lat = str(lat_degree) + ':' + str(lat_min) + ':' + str(lat_sec)
    
    long_degree = int(longitude)
    long_min = int((longitude - long_degree)*60)
    long_sec = (longitude - long_degree - long_min/60)*60*60
    observer.long = str(long_degree) + ':' + str(long_min) + ':' + str(long_sec)
    
    observer.date = datetime.datetime.utcnow()

    sun = ephem.Sun(observer)
    sun_rising_utc = observer.next_rising(sun)
    sun_setting_utc = observer.next_setting(sun)

    sun_rising_str_raw = str(ephem.localtime(sun_rising_utc).ctime())
    sun_setting_str_raw = str(ephem.localtime(sun_setting_utc).ctime())

    sun_rising = sun_rising_str_raw.split(' ')[3]
    sun_setting = sun_setting_str_raw.split(' ')[3]
    
    return (sun_rising,sun_setting)

