from datetime import timedelta, datetime
import requests,json, datetime, csv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Thongchai Angie',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

def get_weather(city):
    api_key = "8c28834273d1c5b639e0f53eef0da3a2"
    base_url = "https://api.openweathermap.org/data/2.5/weather?"
    complete_url = base_url + "appid=" + api_key + "&q=" + city
    # use JSON to get data
    response = requests.get(complete_url)
    x = response.json()
    # use datetime to indentify local date
    time = datetime.datetime.now()
    t = time.strftime("%x")
    y = x["main"]
    # get temperature and convert from kelvin to celsius
    temp = y["temp"] - 273.15
    # float must be 2 decimal numbers
    current_temperature = '%.2f' % temp
    # air pressure
    current_pressure = y["pressure"]
    # air humility
    current_humidity = y["humidity"]
    z = x["weather"]
    # weather detail
    weather_description = z[0]["description"]
    # sum up any values
    result = city, str(t), str(current_temperature) , str(current_pressure) , str(current_humidity) , str(weather_description)
    return list(result)

def convert(ti):
    name = ti.xcom_pull(task_ids='get_weather')
    arr = np.array(name)
    arr = arr.reshape(1,6)
    df = pd.DataFrame(arr, columns=["city", "date", "temperature", "pressure", "humidity", "weather_description"])
    return df

def con(ti):
    collect = ti.xcom_pull(task_ids='convert')
    con = pd.concat([collect], axis=0)
    return con.to_csv("weather.csv")

def read1(ti):
    select = ti.xcom_pull(task_ids='convert')
    plt.scatter(select['city'], select['temperature'], color='blue')
    plt.ylabel('temperature')
    plt.xlabel('city')
    plt.title("Temperature of cities on 30 april 2023 at 16.30")

    plt.grid(True)
    return plt.show()

def read2(ti):
    select = ti.xcom_pull(task_ids='convert')
    plt.scatter(select['pressure'], select['temperature'], color='red')
    plt.ylabel('temperature')
    plt.xlabel('pressure')
    plt.title("air pressure per temperature on 30 april 2023 at 16.30")

    plt.text(1012, 10.72, 'Stockholm')
    plt.text(1021, 17.71, 'London')
    plt.text(1017, 23.73, 'Madrid')
    plt.text(1011, 19.55, 'Rom')
    plt.text(1014, 23.42, 'Cairo')

    plt.grid(True)
    return plt.show()


def read3(ti):
    select = ti.xcom_pull(task_ids='convert')
    plt.scatter(select['humidity'], select['temperature'], color='green')
    plt.ylabel('temperature')
    plt.xlabel('humidity')
    plt.title("humidity per temperature on 30 april 2023 at 16.30")

    plt.text(37, 10.72, 'Stockholm')
    plt.text(51, 17.71, 'London')
    plt.text(36, 23.73, 'Madrid')
    plt.text(71, 19.55, 'Rom')
    plt.text(33, 23.42, 'Cairo')
    plt.grid(True)
    return plt.show()

with DAG(
    dag_id='orchestra',
    default_args = default_args,
    description='ETLassignment',
    start_date = datetime(2023, 05, 6),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id= 'get_weather',
        python_callable= get_weather,
        op_kwargs={'city':'London'}
    )
    task2 = PythonOperator(
        task_id= 'get_weather',
        python_callable= get_weather,
        op_kwargs={'city':'Stockholm'}
    )
    task3 = PythonOperator(
        task_id= 'get_weather',
        python_callable= get_weather,
        op_kwargs={'city':'Madrid'}
    )
    task4 = PythonOperator(
        task_id= 'get_weather',
        python_callable= get_weather,
        op_kwargs={'city':'Rom'}
    )
    task5 = PythonOperator(
        task_id= 'get_weather',
        python_callable= get_weather,
        op_kwargs={'city':'Cairo'}
    )

    task6 = PythonOperator(
        task_id= 'convert',
        python_callable= convert
    )
    task7 = PythonOperator(
        task_id='con',
        python_callable=con
    )
    task8 = PythonOperator(
        task_id='read1',
        python_callable=read1
    )
    task9 = PythonOperator(
        task_id='read2',
        python_callable=read2
    )
    task10 = PythonOperator(
        task_id='read3',
        python_callable=read3
    )

    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task9 >> task10

