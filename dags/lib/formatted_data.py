import os
import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


def prepare_consumption_data(get_consumption, spark):
    consumption_data = get_consumption
    fields = [record['fields'] for record in consumption_data['records']]
    df = spark.createDataFrame(fields)
    return df


def prepare_weather_data(get_daily_weather, spark):
    weather_data = get_daily_weather
    fields = [record['fields'] for record in weather_data['records']]
    df = spark.createDataFrame(fields)
    return df


def prepare_pollution_data(get_pollution, spark):
    pollution_data = get_pollution
    new_pollution = [{'dt': datetime.datetime.fromtimestamp(item['dt']).strftime("%Y-%m-%d"), 'aqi': item['main']['aqi'], **item['components']} for item in pollution_data['list']]
    pollution_dates_cleaned = []
    encountered_dates = set()

    schema = StructType([
        StructField("dt", StringType(), nullable=False),
        StructField("aqi", LongType(), nullable=False),
        StructField("co", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
        StructField("no", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
        StructField("no2", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
        StructField("o3", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
        StructField("so2", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
        StructField("pm2_5", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
        StructField("pm10", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
        StructField("nh3", DoubleType(), nullable=True),  # Replace "component1" with the actual column name
    ])
    for item in new_pollution:
        dt = item['dt']
        if dt not in encountered_dates:
            encountered_dates.add(dt)
            pollution_dates_cleaned.append(item)

    def convert_to_double_or_integer(value):
        try:
            return float(value)
        except ValueError:
            return int(value)

    data_double_or_int = [
        {
            'dt': item['dt'],
            'aqi': item['aqi'],
            'co': convert_to_double_or_integer(item['co']) if 'co' in item else None,
            'no': convert_to_double_or_integer(item['no']) if 'no' in item else None,
            'no2': convert_to_double_or_integer(item['no2']) if 'no2' in item else None,
            'o3': convert_to_double_or_integer(item['o3']) if 'o3' in item else None,
            'so2': convert_to_double_or_integer(item['so2']) if 'so2' in item else None,
            'pm2_5': convert_to_double_or_integer(item['pm2_5']) if 'pm2_5' in item else None,
            'pm10': convert_to_double_or_integer(item['pm10']) if 'pm10' in item else None,
            'nh3': convert_to_double_or_integer(item['nh3']) if 'nh3' in item else None
        }
        for item in pollution_dates_cleaned
    ]

    df = spark.createDataFrame(data_double_or_int, schema)
    return df


def save_as_parquet(format_data, get_data, spark, path):
    df = format_data(get_data, spark)
    relative_path = path
    dag_file_path = os.path.realpath(__file__)
    dag_directory = os.path.dirname(dag_file_path)
    output_path = os.path.join(dag_directory, relative_path)
    df.write.mode("overwrite").parquet(output_path)
    return output_path
