from datetime import datetime
import uuid
from airflow import DAG
import json
import requests
from kafka import KafkaProducer
import time
import logging
from airflow.operators.python import PythonOperator


default_args = {
    'owner':'arvind',
    'start_date': datetime(2024,10,16,9,30)
}


def get_data():
    response = requests.get('https://randomuser.me/api/')
    response = response.json()['results'][0]
    #print(json.dumps(response, indent=2))
    return response

def format_data(response):
    data = {}
    location = response['location']
    data['id'] = str(uuid.uuid4())
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
    #print("Formatted data:", json.dumps(data, indent=2)) 
    return data
    # except Exception as e:
    #     logging.error(f"cant format {e}")

def data_stream():

    producer = KafkaProducer(bootstrap_servers = ['broker:29092'], max_block_ms = 5000)
    current_time = time.time()

    while time.time() < current_time + 600:
        try:
            response = get_data()
            formatted_data = format_data(response)
            producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))

        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue

    


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         ) as dag:
    
    user_automation = PythonOperator( task_id = 'Data_stream',python_callable= data_stream)



if __name__ == "__main__":
    x = get_data()
    print(format_data(x))
    data_stream()