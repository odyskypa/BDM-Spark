from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import col, struct
from pyspark.sql.types import *


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
            db.create_collection(collection_name)

            # Log collection information
            self.logger.info(f"Collection '{collection_name}' created in database '{database_name}'")

        except Exception as e:
            self.logger.error(
                f"An error occurred during the creation of the collection: {collection_name} in MongoDB database:"
                f" {database_name}. The error is: {e}")

        finally:
            # Close the client connection
            client.close()

    def read_mongo_collection(self, db_name, collection_name):
        uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{db_name}.{collection_name}"
        df = self.spark.read.format("mongo") \
            .option('uri', uri) \
            .option('encoding', 'utf-8-sig') \
            .load()
        return df

    def write_to_mongo_collection(self, db_name, collection_name, dataframe, partition = True, append = True):
        uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{db_name}.{collection_name}"
        if partition and append:
            dataframe.write.format("mongo") \
                .option('uri', uri) \
                .option('encoding', 'utf-8-sig') \
                .mode("append") \
                .partitionBy("_id") \
                .save()
        elif partition and not append:
            dataframe.write.format("mongo") \
                .option('uri', uri) \
                .option('encoding', 'utf-8-sig') \
                .mode("rewrite") \
                .partitionBy("_id") \
                .save()
        elif not partition and not append:
            dataframe.write.format("mongo") \
                .option('uri', uri) \
                .option('encoding', 'utf-8-sig') \
                .mode("rewrite") \
                .save()


    def merge_dataframes(self, input_collection1, input_collection2):
        try:
            self.logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
            x1_df = self.read_mongo_collection(self.persistent_db, input_collection1)

            self.logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
            x2_df = self.read_mongo_collection(self.persistent_db, input_collection2)

            self.logger.info("Merging dataframes...")
            unified_df = x1_df.union(x2_df)

            self.logger.info("Dataframes merged successfully.")
            return unified_df

        except Exception as e:
            self.logger.error(f"An error occurred while merging dataframes: {e}")

    def drop_duplicates(self, dataframe):
        deduplicated_df = dataframe.dropDuplicates()
        self.logger.info("Deduplication completed successfully.")
        return deduplicated_df

    def filter_existing_records(self, result_df, existing_df):
        if existing_df.count() > 0:
            self.logger.info("Filtering out already existing records...")
            result_df = result_df.subtract(existing_df)
        return result_df

    def format_lookup_table(self, output_collection, input_collection1, input_collection2):
        try:
            self.logger.info(f"Formatting Lookup-table {output_collection} and moving it to Formatted Zone.")

            self.create_collection(self.formatted_db, output_collection)

            unified_df = self.merge_dataframes(input_collection1, input_collection2)
            unified_df = self.drop_duplicates(unified_df)

            self.logger.info(f"Reading '{output_collection}' collection from MongoDB...")
            existing_df = self.read_mongo_collection(self.formatted_db, output_collection)

            unified_df = self.filter_existing_records(unified_df, existing_df)

            self.logger.info(f"Saving new records into '{output_collection}' collection in MongoDB...")
            self.write_to_mongo_collection(self.formatted_db, output_collection, unified_df)

            self.logger.info(f'Formatting {output_collection} Lookup Table completed successfully.')

        except Exception as e:
            self.logger.exception(e)

    def reconcile_data_with_lookup(self, input_collection, lookup_collection, reconciled_collection,
                                   input_join_attribute, lookup_join_attribute, lookup_id, input_id_reconcile):
        try:

            self.logger.info(f"Reading input data from MongoDB collection '{input_collection}'...")
            inputDF = self.read_mongo_collection(self.persistent_db, input_collection)

            self.logger.info(f"Reading lookup data from MongoDB collection '{lookup_collection}'...")
            lookupDF = self.read_mongo_collection(self.formatted_db, lookup_collection)
            lookupDF = lookupDF.select(lookup_join_attribute, lookup_id)

            self.logger.info("Performing join and reconciliation...")
            resultDF = inputDF.join(lookupDF, inputDF[input_join_attribute] == lookupDF[lookup_join_attribute], "left") \
                .withColumn(input_id_reconcile, lookupDF[lookup_id]) \
                .drop(lookupDF[lookup_join_attribute]) \
                .drop(lookupDF[lookup_id])

            self.logger.info(f"Reading existing data from MongoDB collection '{reconciled_collection}'...")
            existingDF = self.read_mongo_collection(self.formatted_db, reconciled_collection)

            resultDF = self.filter_existing_records(resultDF, existingDF)

            self.logger.info(f"Writing result DataFrame to MongoDB collection '{reconciled_collection}'...")
            self.write_to_mongo_collection( self.formatted_db, reconciled_collection, resultDF)

            resultDF.show()
        except Exception as e:
            self.logger.error(f"An error occurred during data reconciliation: {e}")


    def change_data_types(self, db_name, collection_name, column_data_types):
        df = self.read_mongo_collection(db_name, collection_name)

        def recursively_cast_columns(df, column_list, data_type):
            if len(column_list) == 1:
                return df.withColumn(column_list[0], col(column_list[0]).cast(data_type))
            else:
                nested_struct = struct(recursively_cast_columns(df, column_list[:-1], data_type).alias(column_list[-2]), col(column_list[-1]).alias(column_list[-1]))
                return df.withColumn(column_list[-2], nested_struct)

        for column, data_type in column_data_types.items():
            if '.' in column:
                # Handle nested structure
                nested_columns = column.split('.')
                df = recursively_cast_columns(df, nested_columns, data_type)
            else:
                df = df.withColumn(column, col(column).cast(data_type))

        self.write_to_mongo_collection(db_name, collection_name, df)
