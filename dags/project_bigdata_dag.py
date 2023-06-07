import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

from lib.electricity import get_electricity_consumption
from lib.formatted_data import prepare_weather_data, save_as_parquet, prepare_consumption_data, prepare_pollution_data
from lib.pollution import get_pollution
from lib.weather import get_daily_weather
from lib.combine import combine_formatted_data
from lib.index_to_elastic import indexing

with DAG(
        'big_data',
        default_args={
            'depends_on_past': False,
            'email': ['adrienbethuel@gmail.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A first DAG',
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    dag.doc_md = """
       Dag for the big data project at ISEP.
       Subject : analysis of electricity consumption and air pollution according to the weather
   """

    spark = SparkSession.builder \
        .appName("JSON to Parquet Conversion") \
        .getOrCreate()

    current_date = datetime.now()
    formatted_date = current_date.strftime("%Y%m%d")
    root_path = os.path.dirname(os.path.abspath(__file__))

    def task_get_weather():
        print("We are getting weather")
        weather = get_daily_weather()
        output_path = os.path.join(root_path, '..', 'datalake/raw/opendatasoft/pollution/' + formatted_date, 'weather.json')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as file:
            json.dump(weather, file)
        print("Weather saved at :", output_path)
        return output_path

    def task_get_pollution():
        print("We are getting pollution")
        pollution = get_pollution()
        output_path = os.path.join(root_path, '..', 'datalake/raw/openweathermap/weather/' + formatted_date, 'pollution.json')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as file:
            json.dump(pollution, file)
        print("Pollution saved at :", output_path)
        return output_path

    def task_get_electricity():
        print("We are getting electricity consumption")
        electricity_consumption = get_electricity_consumption()
        output_path = os.path.join(root_path, '..', 'datalake/raw/opendatasoft/electricity_consumption/' + formatted_date, 'electricity_consumption.json')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as file:
            json.dump(electricity_consumption, file)
        print("Electricity consumption saved at :", output_path)
        return output_path

    def task_format_weather(**context):
        print("formatted weather")
        weather_json_path = context['ti'].xcom_pull(task_ids='task_get_weather')
        with open(weather_json_path) as file:
            weather_data = json.load(file)
        return save_as_parquet(prepare_weather_data, weather_data, spark, "../../datalake/formatted/opendatasoft/weather/" + formatted_date + "/formatted_weather.parquet")

    def task_format_pollution(**context):
        print("formatted pollution")
        pollution_json_path = context['ti'].xcom_pull(task_ids='task_get_pollution')
        with open(pollution_json_path) as file:
            pollution_data = json.load(file)
        return save_as_parquet(prepare_pollution_data, pollution_data, spark, "../../datalake/formatted/openweathermap/pollution/" + formatted_date + "/formatted_pollution.parquet")

    def task_format_electricity(**context):
        print("Formatted consumption")
        consumption_json_path = context['ti'].xcom_pull(task_ids='task_get_electricity')
        with open(consumption_json_path) as file:
            consumption_data = json.load(file)
        return save_as_parquet(prepare_consumption_data, consumption_data, spark, "../../datalake/formatted/opendatasoft/electricity_consumption/" + formatted_date + "/formatted_consumption.parquet")


    def task_solution_usage(**context):
        print("combine exploitable data")
        weather_parquet_path = context['ti'].xcom_pull(task_ids='task_format_weather')
        pollution_parquet_path = context['ti'].xcom_pull(task_ids='task_format_pollution')
        electricity_parquet_path = context['ti'].xcom_pull(task_ids='task_format_electricity')
        usage_df = combine_formatted_data(weather_parquet_path, pollution_parquet_path, electricity_parquet_path, spark)
        dag_file_path = os.path.realpath(__file__)
        dag_directory = os.path.dirname(dag_file_path)
        output_path = os.path.join(dag_directory, "../datalake/usage/pollution_consumption_by_weather/" + formatted_date + "/usage_pollution_consumption_weather.parquet")
        usage_df.write.mode("overwrite").parquet(output_path)
        return output_path


    def task_index_to_elastic(**context):
        usage_parquet_path = context['ti'].xcom_pull(task_ids='task_solution_usage')
        indexing(usage_parquet_path, spark)
        print("We are indexing a wonderful module")

    source_weather_raw = PythonOperator(
        task_id='task_get_weather',
        python_callable=task_get_weather,
    )

    source_pollution_raw = PythonOperator(
        task_id='task_get_pollution',
        python_callable=task_get_pollution
    )

    source_electricity_raw = PythonOperator(
        task_id='task_get_electricity',
        python_callable=task_get_electricity
    )

    formatted_weather = PythonOperator(
        task_id='task_format_weather',
        python_callable=task_format_weather
    )

    formatted_pollution = PythonOperator(
        task_id='task_format_pollution',
        python_callable=task_format_pollution
    )

    formatted_electricity = PythonOperator(
        task_id='task_format_electricity',
        python_callable=task_format_electricity
    )

    solution_usage = PythonOperator(
        task_id='task_solution_usage',
        python_callable=task_solution_usage
    )

    index_to_elastic = PythonOperator(
        task_id="task_index_to_elastic",
        python_callable=task_index_to_elastic
    )

    source_weather_raw >> formatted_weather
    source_pollution_raw >> formatted_pollution
    source_electricity_raw >> formatted_electricity

    [formatted_weather, formatted_pollution, formatted_electricity] >> solution_usage >> index_to_elastic





