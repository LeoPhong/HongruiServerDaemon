import urllib.request
import json
import pprint


def getLocationFromLatLong(latitude,longitude):
    api_key = 'eo2Sudm8I1UQpj5h9rB7BGrSTR3nh6ye'
    #latitude = 27.735236
    #longitude = 111.360324
    url = "http://api.map.baidu.com/geocoder/v2/?ak=" + api_key + "&callback=renderReverse&location=" + str(latitude) + "," + str(longitude) +"&output=json&pois=0"
    request_handle = urllib.request.urlopen(url, timeout = 5)
    response_str = request_handle.read().decode('utf-8')
    request_handle.close()
    response_json = json.loads(response_str[29:-1])
    country = response_json['result']['addressComponent']['country']
    province = response_json['result']['addressComponent']['province']
    city = response_json['result']['addressComponent']['city']
    district = response_json['result']['addressComponent']['district']
    street = response_json['result']['addressComponent']['street']
    print((country,province,city,district,street))



if __name__ == '__main__':
    getLocationFromLatLong(0,0)
