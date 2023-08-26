
import logging
import requests

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.models.xcom import XCom


url = 'https://api.weatherapi.com/v1/forecast.json?key=1a61a96be1274fd5a92181834221312&q=Samara&days=1&aqi=no&alerts=no'

class Ra_Valiev_Weather_Operator(BaseOperator):
    
    def __init__(self, **kwargs) -> None:
        super().__init__( **kwargs)



    def get_weather_api(self):
        response = requests.get(url)
        if response.status_code == 200:
            print('success')
            return response.json()
        else:
            logging.warning(f'Http status code is {response.status_code}')
            raise AirflowException("Warning: Can't get info from api.weatherapi.com")
        
    def execute(self, context):
        
        resp_json = self.get_weather_api()
        
        lst = []
        city = {'city': resp_json['location']['name']}
        country = {'country': resp_json['location']['country']}
        local_time = {'local_time': resp_json['location']['localtime']}
        last_updated = {'last_updated': resp_json['current']['last_updated']}
        temp_c = {'temp_c': resp_json['current']['temp_c']}
        condition = {'condition': resp_json['current']['condition']['text']}
        wind_kph = {'wind_kph': resp_json['current']['wind_kph']}
        gust_kph = {'gust_kph': resp_json['current']['gust_kph']}
        humidity = {'humidity': resp_json['current']['humidity']}
        cloudness = {'cloudness': resp_json['current']['cloud']}
        feelslike = {'feelslike': resp_json['current']['feelslike_c']}
        visible = {'visible': resp_json['current']['vis_km']}

        lst.append(city)
        lst.append(country)
        lst.append(local_time)
        lst.append(last_updated)
        lst.append(temp_c)
        lst.append(condition)
        lst.append(wind_kph)
        lst.append(gust_kph)
        lst.append(humidity)
        lst.append(cloudness)
        lst.append(feelslike)
        lst.append(visible)    
    
        context['ti'].xcom_push(value=lst, key='weather_list')