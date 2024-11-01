from datetime import datetime
from airflow.decorators import dag, task
import json
import requests
from kafka import KafkaProducer
import time
import logging


default_args = {
    'owner':'arvind',
    'start_date': datetime(2024,10,16,9,30)
}


@task
def get_data():
    response = requests.get('https://randomuser.me/api/')
    response = response.json()['results'][0]
    # print(json.dumps(response, indent=2))
    return response

@task
def format_data(response):
    data = {}
    location = response['location']
    data['first_name'] = response['name']["first"]
    data['last_name'] = response['name']['last']
    data['username'] = response['login']['username']
    data['age'] = response['dob']['age']
    data['gender'] = response['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['pincode'] = location['postcode']
    data['email'] = response['email']
    data['phone_number'] = response['phone']
    data['registered_date'] = response['registered']['date']
    data['image'] = response['picture']['medium']
    
    return data

@task
def data_stream(data):

    producer = KafkaProducer(bootstrap_servers = ['broker:29092'], max_block_ms = 5000)
    current_time = time.time()

    while time.time() < current_time + 60:
        try:
            producer.send('users_created', json.dumps(data).encode('utf-8'))

        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue

    #print(json.dumps(response, indent=2))

    

@dag(
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags =['user_automation']    
)
    
def user_automation():
    response = get_data()
    formatted_data = format_data(response)
    data_stream(formatted_data)


user_automation_dag = user_automation()