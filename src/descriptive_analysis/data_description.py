from pyspark.sql import SparkSession
from src.utils.mongo_utils import MongoDBUtils

class DataDescription:

    def __init__(self, logger, vm_host, mongodb_port, persistent_db, formatted_db, exploitation_db):
        self.logger = logger
        self.vm_host = vm_host
        self.mongodb_port = mongodb_port
        self.persistent_db = persistent_db
        self.formatted_db = formatted_db
        self.exploitation_db = exploitation_db

        try:
            # Create a SparkSession
            self.spark = SparkSession.builder \
                .master("local[*]") \
                .appName("Unify Lookup District") \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                .getOrCreate()
        except Exception as e:
            self.logger.error(
                f"An error occurred during the creation of the Spark Configuration during the creation of DataFormatter class."
                f" This is the error: {e}")