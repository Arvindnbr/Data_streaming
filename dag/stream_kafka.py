from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default = {
    'owner':'arvind',
    'start_date': datetime(2024,10,16,9,30)
}


def get_data():
    import json
    import requests

    response = requests.get('https://randomuser.me/api/')
    response = response.json()['results'][0]
    # print(json.dumps(response, indent=2))
    return response

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

def data_stream():
    import json
    response = get_data()
    response = format_data(response=response)

    print(json.dumps(response, indent=2))





# with DAG('user_automation',
#          default_args=default,
#          schedule_interval='@daily',
#          catchup=False) as dag:
    
#     streaming = PythonOperator(
#         task_id = 'stream_from_api',
#         python_callable= data_stream
#     )


data_stream()