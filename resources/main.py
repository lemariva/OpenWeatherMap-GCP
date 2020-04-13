"""
Copyright 2020 LeMaRiva|Tech (Mauro Riva) info@lemariva.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import requests
import logging
import sys
import json
from google.cloud import pubsub_v1
import time

LOOP_TIME_SLEEP = 60 * 10

def get_weather(api_key, lat, lon):
    url = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&units=metric&appid={}".format(lat, lon, api_key)
    logging.info('Getting the weather...')
    r = requests.get(url)
    return r.json()

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        logging.info('Publishing message threw an Exception {}.'.format(message_future.exception()))
    else:
        logging.info(message_future.result())

def run(argv=None):
    parser = argparse.ArgumentParser()

    publisher = pubsub_v1.PublisherClient()
    
    parser.add_argument(
      '--api', dest='api', required=True, help='OpenWeatherMap API')
    
    parser.add_argument(
      '--location', dest='location', required=True, help='Location "lat,lon"')

    parser.add_argument(
      '--project', dest='project', default='core-iot-sensors', help='Project Name')

    parser.add_argument(
      '--topic', dest='topic', default='openweathermap-topic', help='Topic Name')

    known_args, pipeline_args = parser.parse_known_args(argv)
    
    api_key = known_args.api
    location = known_args.location
    lat_long = tuple(map(float, location.split(','))) 

    project_id = known_args.project
    topic_name = known_args.topic

    topic_path = publisher.topic_path(project_id, topic_name)

    starttime = time.time()

    last_dt = 0

    logging.info('Starting the loop...')

    while True:
        weather = get_weather(api_key, lat_long[0], lat_long[1])

        if last_dt != weather["dt"]:
            message = {}
            message["device_id"] = "owm-service"
            message["timestamp"] = weather["dt"]
            message["sunrise"] = weather["sys"]["sunrise"]
            message["sunset"] = weather["sys"]["sunset"]
            message["id"] = weather["weather"][0]["id"]
            message["main"] = weather["weather"][0]["main"]
            message["icon"] = weather["weather"][0]["icon"]
            message["temp"] = weather["main"]["temp"]
            message["feels_like"] = weather["main"]["feels_like"]
            message["humidity"] = weather["main"]["humidity"]
            message["visibility"] = weather["visibility"]
            message["w_speed"] = weather["wind"]["speed"]
            if "deg" in weather["wind"]:
                message["w_deg"] = weather["wind"]["deg"]
            
            if "clouds" in weather:
                message["all_clouds"] = weather["clouds"]["all"]
            if "rain" in weather:
                if "1h" in weather["rain"]:  
                    message["rain_1h"] = weather["rain"]["1h"]
                if "3h" in weather["rain"]:  
                    message["rain_3h"] = weather["rain"]["3h"]
            if "snow" in weather:
                if "1h" in weather["snow"]:  
                    message["snow_1h"] = weather["snow"]["1h"]
                if "3h" in weather["snow"]:  
                    message["snow_3h"] = weather["snow"]["3h"]

            data = json.dumps(message).encode('utf-8')
            message_future = publisher.publish(topic_path, data=data)
            message_future.add_done_callback(callback)
            last_dt = weather["dt"]

        time.sleep(LOOP_TIME_SLEEP - ((time.time() - starttime) % LOOP_TIME_SLEEP))
  
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()