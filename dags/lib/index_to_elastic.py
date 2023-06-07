from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from dotenv import load_dotenv
import os


def indexing(path, spark):
    load_dotenv()
    elastic_password = os.getenv("ELASTIC_PASSWORD")
    print(elastic_password)
    client = Elasticsearch(hosts="https://isep-big-data-c985e0.es.us-central1.gcp.cloud.es.io:9243",
                           basic_auth=('elastic', elastic_password))
    df = spark.read.parquet(path)

    def index_row(row):
        document = row.asDict()
        document["timestamp"] = datetime.utcnow()
        return document

    docs = df.rdd.map(index_row).collect()
    helpers.bulk(client, docs, index="weather_pollution_consumption")



