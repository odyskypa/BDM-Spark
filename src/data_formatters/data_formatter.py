from pyspark.sql import SparkSession
from pymongo import MongoClient
class DataFormatter:

    def __init__(self, logger, vm_host, mongodb_port, persistent_db, formatted_db):
        self.logger = logger
        self.vm_host = vm_host
        self.mongodb_port = mongodb_port
        self.persistent_db = persistent_db
        self.formatted_db = formatted_db

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

    def create_collection(self, database_name, collection_name):
        # Connect to MongoDB
        client = MongoClient(self.vm_host, int(self.mongodb_port))
        try:
            # Access the database
            db = client[database_name]

            # Create a new collection
            collection = db[collection_name]

            # Log collection information
            self.logger.info(f"Collection '{collection_name}' created in database '{database_name}'")
            client.close()
        except Exception as e:
            self.logger.error(
                f"An error occurred during the creation of the collection: {collection_name} in MongoDB database:"
                f" {database_name}."f"The error is: {e}")
            client.close()

    def merge_two_collections_and_drop_duplicates(self, input_collection1, input_collection2, output_collection):
        try:
            self.logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
            x1_df = self.spark.read.format("mongo") \
                .option('uri', f"mongodb://{self.vm_host}:{self.mongodb_port}/{self.persistent_db}.{input_collection1}") \
                .load()
            self.logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
            x2_df = self.spark.read.format("mongo") \
                .option('uri', f"mongodb://{self.vm_host}:{self.mongodb_port}/{self.persistent_db}.{input_collection2}") \
                .load()

            self.logger.info("Merging and deduplicating DataFrames...")
            unified_df = x1_df.union(x2_df).distinct()

            self.logger.info(f"Saving unified data into '{output_collection}' collection in MongoDB...")
            unified_df.write.format("mongo") \
                .option('uri', f"mongodb://{self.vm_host}:{self.mongodb_port}/{self.formatted_db}.{output_collection}") \
                .mode("overwrite") \
                .save()

            self.logger.info("Data merge and deduplication completed successfully.")
        except Exception as e:
            self.logger.error(f"An error occurred while merging and deduplicating collections: {e}")

    def format_lookup_tables(self):
        try:
            self.logger.info(f"Formatting Lookup-tables and moving them to Formatted Zone.")

            self.create_collection(self.formatted_db, "lookup_tables_district")
            self.create_collection(self.formatted_db, "lookup_tables_neighborhood")

            self.merge_two_collections_and_drop_duplicates("income_lookup_district",
                                                           "rent_lookup_district",
                                                           "lookup_tables_district")

            self.merge_two_collections_and_drop_duplicates("income_lookup_neighborhood",
                                                           "rent_lookup_neighborhood",
                                                           "lookup_tables_neighborhood")

            self.logger.info('Formatting District and Neighborhood Lookup Tables completed successfully.')

        except Exception as e:
            self.logger.exception(e)

    def reconcile_data_with_lookup(self, input_collection, lookup_collection, reconciled_collection,
                                   input_join_attribute, lookup_join_attribute, lookup_id, input_id_reconcile):
        try:
            self.logger.info(f"Initializing SparkSession for data reconciliation...")

            self.logger.info(f"Reading input data from MongoDB collection '{input_collection}'...")
            inputDF = self.spark.read.format("mongo") \
                .option('uri', f"mongodb://{self.vm_host}/{input_collection}") \
                .option('encoding', 'utf-8-sig') \
                .load()

            self.logger.info(f"Reading lookup data from MongoDB collection '{lookup_collection}'...")
            lookupDF = self.spark.read.format("mongo") \
                .option('uri', f"mongodb://{self.vm_host}/{lookup_collection}") \
                .option('encoding', 'utf-8-sig') \
                .load()
            lookupDF = lookupDF.select(lookup_join_attribute, lookup_id)

            self.logger.info("Performing join and reconciliation...")
            resultDF = inputDF.join(lookupDF, inputDF[input_join_attribute] == lookupDF[lookup_join_attribute], "left") \
                .withColumn(input_id_reconcile, lookupDF[lookup_id]) \
                .drop(lookupDF[lookup_join_attribute]) \
                .drop(lookupDF[lookup_id])

            self.logger.info(f"Writing result DataFrame to MongoDB collection '{reconciled_collection}'...")
            resultDF.write.format("mongo") \
                .option('uri', f"mongodb://{self.vm_host}/{reconciled_collection}") \
                .mode("overwrite") \
                .save()

            resultDF.show()
        except Exception as e:
            self.logger.error(f"An error occurred during data reconciliation: {e}")
