from datetime import datetime
from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import col, max as spark_max, to_date, concat, lit, explode, levenshtein, when

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

    def drop_mongo_collection(self, db_name, collection_name):
        # Connect to MongoDB
        client = MongoClient(self.vm_host, int(self.mongodb_port))
        try:

            # Access the specified database
            db = client[db_name]

            # Drop the collection
            db[collection_name].drop()

            # Close the MongoDB client
            client.close()

        except Exception as e:
            self.logger.error(
                f"An error occurred during the dropping of the collection: {collection_name} in MongoDB database:"
                f" {db_name}. The error is: {e}")

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

    def write_to_mongo_collection(self, db_name, collection_name, dataframe, append = True):
        uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{db_name}.{collection_name}"

        if not append:
            self.drop_mongo_collection(db_name, collection_name)

        dataframe.write.format("mongo") \
            .option("uri", uri) \
            .option("encoding", "utf-8-sig") \
            .mode("append") \
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
        self.logger.info("Filtering out already existing records...")
        if not existing_df.isEmpty:
            existing_ids = existing_df.select("_id").collect()
            result_df = result_df.filter(~result_df["_id"].isin([row["_id"] for row in existing_ids]))
        return result_df

    def merge_lookup_table(self, output_collection, input_collection1, input_collection2):
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

    def transform_idealista_to_latest_info(self, db_name, collection_name):

        try:
            # Load the MongoDB collection into a PySpark DataFrame
            df = self.read_mongo_collection(db_name, collection_name)

            # Explode the 'value' array
            exploded_df = df.withColumn("exploded_value", explode(col("value"))).drop("value")

            # Transform the collection
            transformed_df = exploded_df.selectExpr("_id as date", "exploded_value.*") \
                .withColumn("new_id", concat(col("date"), lit("_"), col("propertyCode"))) \
                .withColumnRenamed("new_id", "_id")

            transformed_df = transformed_df.withColumn("date", to_date(col("date"), "yyyy_MM_dd"))

            latest_dates = transformed_df.groupBy("propertyCode").agg(spark_max("date").alias("latestDate"))

            # Perform a left semi join to filter the transformed_df based on the latest dates
            filtered_df = transformed_df.join(latest_dates,
                                              (transformed_df.propertyCode == latest_dates.propertyCode) & (
                                                          transformed_df.date == latest_dates.latestDate), "left_semi")
            # Drop the _id column
            filtered_df = filtered_df.drop("_id")

            # Rename the propertyCode column to _id
            filtered_df = filtered_df.withColumnRenamed("propertyCode", "_id")

            # Reorder the columns to make _id the first column
            filtered_df = filtered_df.select("_id", *filtered_df.columns[:-1])

            return filtered_df

            self.logger.info("Transformation completed successfully.")
        except Exception as e:
            self.logger.error("Error occurred during transformation: %s", str(e))

    def convert_collection_data_types(self, db_name_input, db_name_output, collection_name, new_schema):
        # Read the collection from MongoDB
        df = self.read_mongo_collection(db_name_input, collection_name)

        # Apply the new schema to the DataFrame
        df_with_new_schema = df
        for field in new_schema.fields:
            if field.name in df.columns:
                df_with_new_schema = df_with_new_schema.withColumn(field.name, df[field.name].cast(field.dataType))

        # Write the DataFrame back to the same collection
        self.write_to_mongo_collection(db_name_output, collection_name, df_with_new_schema, True)

    def reconcile_data_with_lookup(self, input_dF, lookup_df, input_join_attribute, lookup_join_attribute, lookup_id, input_id_reconcile, threshold):
        try:

            self.logger.info("Performing join and reconciliation...")
            resultDF = input_dF.join(lookup_df,
                                    when(
                                        levenshtein(col(input_join_attribute),
                                                    col(lookup_join_attribute)) <= threshold,
                                        True).otherwise(False),
                                    "left"
                                    ).withColumn(input_id_reconcile, lookup_df[lookup_id]) \
                .drop(lookup_df[lookup_join_attribute]) \
                .drop(lookup_df[lookup_id])

            resultDF.show()
            return resultDF
        except Exception as e:
            self.logger.error(f"An error occurred during data reconciliation: {e}")