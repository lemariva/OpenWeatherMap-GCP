import argparse
import requests
import logging
import sys
from google.cloud import pubsub_v1
import time

LOOP_TIME_SLEEP = 60 * 10

def get_weather(api_key, lat, lon):
    url = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&units=metric&appid={}".format(lat, lon, api_key)
    r = requests.get(url)
    return r.json()

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message threw an Exception {}.'.format(message_future.exception()))
    else:
        print(message_future.result())

def main(argv=None):

    publisher = pubsub_v1.PublisherClient()
    parser = argparse.ArgumentParser()
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

    while True:
        weather = get_weather(api_key, lat_long[0], lat_long[1])
    
        weather_small = {}
        weather_small["timestamp"] = weather["dt"]
        weather_small["sunrise"] = weather["sys"]["sunrise"]
        weather_small["sunset"] = weather["sys"]["sunset"]
        weather_small["id"] = weather["weather"][0]["id"]
        weather_small["main"] = weather["weather"][0]["main"]
        weather_small["icon"] = weather["weather"][0]["icon"]
        weather_small["temp"] = weather["main"]["temp"]
        weather_small["feels_like"] = weather["main"]["feels_like"]
        weather_small["humidity"] = weather["main"]["humidity"]
        weather_small["visibility"] = weather["visibility"]
        weather_small["w_speed"] = weather["wind"]["speed"]
        weather_small["w_deg"] = weather["wind"]["deg"]
        
        if "clouds" in weather:
            weather_small["all_clouds"] = weather["clouds"]["all"]
        if "rain" in weather:
            if "1h" in weather["rain"]:  
                weather_small["rain_1h"] = weather["rain"]["1h"]
            if "3h" in weather["rain"]:  
                weather_small["rain_3h"] = weather["rain"]["3h"]
        if "snow" in weather:
            if "1h" in weather["snow"]:  
                weather_small["snow_1h"] = weather["snow"]["1h"]
            if "3h" in weather["snow"]:  
                weather_small["snow_3h"] = weather["snow"]["3h"]

        data = str(weather_small).encode('utf-8')
        message_future = publisher.publish(topic_path, data=data)
        message_future.add_done_callback(callback)

        time.sleep(LOOP_TIME_SLEEP - ((time.time() - starttime) % LOOP_TIME_SLEEP))
  
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()