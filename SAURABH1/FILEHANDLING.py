from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functions import *
from sys import argv
import json

spark=SparkSession.builder.appName("SAmple").getOrCreate()

config_path="D://config.json"

def read_config(json_file_path):
    with open("config_path","r") as config_data:
        c_data=json.load(config_data)
    return c_data



