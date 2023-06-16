from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.utils.mongo_utils import MongoDBUtils

class DataModeling:

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

    def get_data_from_formatted_to_exploitation(self):
        # Read idealista_reconciled collection and select relevant columns
        idealista_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "idealista_reconciled"
        ).select("_id", "size", "rooms", "bathrooms", "latitude", "longitude", "exterior", "floor", "has360",
            "has3DTour", "hasLift", "hasPlan", "hasStaging", "hasVideo","neighborhood_id", "numPhotos", "price") \
            .filter(col("municipality") == "Barcelona") \
            .filter(col("neighborhood_id").isNotNull())



        # Read income_reconciled collection and select relevant columns
        income_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "income_reconciled"
        ).select("_id", "info.year", "info.RFD")

        # Read buildin_age_reconciled collection and select relevant columns
        buildin_age_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "building_age_reconciled"
        ).select("_id", "info.year", "info.mean_age")

        # Join the three dataframes on district_id
        joined_df = idealista_df.join(
            income_df,
            idealista_df["neighborhood_id"] == income_df["_id"],
            "left"
        ).drop(income_df["_id"]).withColumnRenamed("year", "income_year")

        #joined_df.show()

        joined_df = joined_df.join(
            buildin_age_df,
            joined_df["neighborhood_id"] == buildin_age_df["_id"],
            "left"
        ).drop(buildin_age_df["_id"]).withColumnRenamed("year", "building_year")

        #joined_df.show()

        self.logger.info('Data sources joined successfully.')

        # Save the joined dataframe to a new collection in MongoDB
        MongoDBUtils.write_to_collection(
            self.logger,
            self.vm_host,
            self.mongodb_port,
            self.exploitation_db,
            "model_collection",
            joined_df
        )