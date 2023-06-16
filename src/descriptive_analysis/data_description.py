from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
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
            self.spark = SparkSession.builder \
                .master("local[*]") \
                .appName("Merge Configurations") \
                .config('spark.jars.packages',
                        'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.6.0') \
                .getOrCreate()

        except Exception as e:
            self.logger.error(
                f"An error occurred during the creation of the Spark Configuration during the creation of DataFormatter class."
                f" This is the error: {e}")


    # KPI 1 AND 2 --> Correlation of rent price and family income per neighborhood / Average number of new listings per day and per neighborhood
    def get_data_from_formatted_to_exploitation(self):
        # Read idealista_reconciled collection and select relevant columns
        idealista_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "idealista_reconciled"
        ).select("_id", "neighborhood_id", "price", "date", "neighborhood_name","latitude", "longitude", "district_name") \
            .filter(col("municipality") == "Barcelona") \
            .filter(col("neighborhood_id").isNotNull()) \

        # Read income_reconciled collection and select relevant columns
        income_df = MongoDBUtils.read_collection(
            self.logger,
            self.spark,
            self.vm_host,
            self.mongodb_port,
            self.formatted_db,
            "income_reconciled"
        ).select("_id", "info.year", "info.RFD") \

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
        joined_df = joined_df.withColumn('RFD', expr(
            'aggregate(RFD, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / size(RFD)'))

        joined_df = joined_df.join(
           buildin_age_df,
           joined_df["neighborhood_id"] == buildin_age_df["_id"],
           "left"
        ).drop(buildin_age_df["_id"]).withColumnRenamed("year", "building_year")
        joined_df = joined_df.withColumn('mean_age', expr(
            'aggregate(mean_age, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / size(mean_age)'))

        self.logger.info('Data sources joined successfully.')

        database_url = "jdbc:postgresql://10.4.41.68:5432/exploitation"
        table_name = "kpi1_2"
        properties = {
            "user": "bdm",
            "password": "bdm",
            "driver": "org.postgresql.Driver"
        }

        joined_df.write.jdbc(url=database_url, table=table_name, mode="overwrite", properties=properties)