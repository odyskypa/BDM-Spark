import os
import argparse
import findspark
import logging.handlers
from pymongo import MongoClient
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.data_formatters.data_formatter import DataFormatter

# Create logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler which logs debug messages
log_file = os.path.join('logs', 'main.log')
log_dir = os.path.dirname(log_file)

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=5)
file_handler.setLevel(logging.DEBUG)

# Create console handler which logs info messages
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Initialize Spark Locally
findspark.init()

# Load environment variables from config..env
load_dotenv()

# Define VM_HOST AND VM_USER parameters from environment variables
VM_HOST = os.getenv('VM_HOST')
VM_USER = os.getenv('VM_USER')

# Define MongoDB parameters from environment variables
MONGODB_PORT = os.getenv('MONGODB_PORT')
FORMATTED_DB = os.getenv('FORMATTED_DB')
PERSISTENT_DB = os.getenv('PERSISTENT_DB')
LOOKUP_TABLES_DISTRICT_FORMATTED_COLLECTION = os.getenv('LOOKUP_TABLES_DISTRICT_FORMATTED_COLLECTION')

def create_collection(host, port, database_name, collection_name):
    # Connect to MongoDB
    client = MongoClient(host, int(port))
    try:

        # Access the database
        db = client[database_name]

        # Create a new collection
        collection = db[collection_name]

        # Log collection information
        logger.info(f"Collection '{collection_name}' created in database '{database_name}'")
        client.close()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        client.close()


def merge_two_collections_and_drop_duplicates(spark, vm_host, mongodb_port, input_db, input_collection1,
                                              input_collection2,
                                              output_db,
                                              output_collection
                                              ):
    # Read the rent_lookup_district collection
    x1_df = spark.read.format("mongo") \
        .option('uri', f"mongodb://{vm_host}:{mongodb_port}/{input_db}.{input_collection1}") \
        .load()

    # Read the income_lookup_district collection
    x2_df = spark.read.format("mongo") \
        .option('uri', f"mongodb://{vm_host}:{mongodb_port}/{input_db}.{input_collection2}") \
        .load()

    # Union the two DataFrames and remove duplicates
    unified_df = x1_df.union(x2_df).distinct()

    # Save the unified data into the new collection
    unified_df.write.format("mongo") \
        .option('uri',
                f"mongodb://{vm_host}:{mongodb_port}/{output_db}.{output_collection}") \
        .mode("overwrite") \
        .save()


def main():

    # Create argument parser
    parser = argparse.ArgumentParser(description='Temporal Landing Zone')

    # Add argument for execution mode
    parser.add_argument('exec_mode', type=str, choices=['data-formatting', 'persistence-loading'],
                        help='Execution mode')

    # Parse command line arguments
    args = parser.parse_args()
    exec_mode = args.exec_mode

    if exec_mode == 'data-formatting':

        try:
            # Initialize a DataCollector instance
            data_formatter = DataFormatter(
                logger)

            # Run the data collection functions
            # data_collector.upload_csv_files_to_hdfs(TEMPORAL_LANDING_CSV_DIR_PATH)
            #             # data_collector.upload_json_files_to_hdfs(TEMPORAL_LANDING_JSON_DIR_PATH)
            #             # data_collector.download_from_opendata_api_to_hdfs()

            # spark = SparkSession \
            #     .builder \
            #     .master(f"local[*]") \
            #     .appName("myApp") \
            #     .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            #     .getOrCreate()
            #
            # create_collection(VM_HOST, MONGODB_PORT, FORMATTED_DB, LOOKUP_TABLES_DISTRICT_FORMATTED_COLLECTION)
            #
            # idealistaRDD = spark.read.format("mongo") \
            #     .option('uri', f"mongodb://{VM_HOST}/persistent.idealista") \
            #     .load() \
            #     .rdd
            #
            # #idealistaRDD.foreach(lambda r: print(r))
            # print(idealistaRDD.take(3))

            # Create a SparkSession
            spark = SparkSession.builder \
                .master(f"local[*]") \
                .appName("Unify Lookup District") \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                .getOrCreate()


            create_collection(VM_HOST, MONGODB_PORT, FORMATTED_DB, "lookup_tables_district")
            create_collection(VM_HOST, MONGODB_PORT, FORMATTED_DB, "lookup_tables_neighborhood")

            merge_two_collections_and_drop_duplicates(spark, VM_HOST, MONGODB_PORT, PERSISTENT_DB,
                                                      "income_lookup_district",
                                                      "rent_lookup_district",
                                                      FORMATTED_DB,
                                                      "lookup_tables_district")

            merge_two_collections_and_drop_duplicates(spark, VM_HOST, MONGODB_PORT, PERSISTENT_DB,
                                                      "income_lookup_neighborhood",
                                                      "rent_lookup_neighborhood",
                                                      FORMATTED_DB,
                                                      "lookup_tables_neighborhood")



            logger.info('Formatting District and Neighborhood Lookup Tables completed successfully.')

        except Exception as e:

            logger.exception(f'Error occurred during data collection: {e}')

    # elif exec_mode == 'persistence-loading':
    #
    #     try:
    #         # Initialize a PersistenceLoader instance
    #         persistence_loader = PersistenceLoader(
    #             HDFS_HBASE_HOST,
    #             HBASE_PORT,
    #             HDFS_PORT,
    #             HDFS_USER,
    #             TEMPORAL_LANDING_DIR_PATH,
    #             TEMPORAL_LANDING_CSV_DIR_PATH,
    #             TEMPORAL_LANDING_JSON_DIR_PATH,
    #             logger)
    #
    #         # Run the persistence loader functions per source
    #
    #         persistence_loader.load_opendatabcn_income()
    #         persistence_loader.load_veh_index_motoritzacio()
    #         persistence_loader.load_lookup_tables()
    #         persistence_loader.load_idealista()
    #
    #         # Terminate connection with HBase
    #         persistence_loader.close()
    #
    #         logger.info('Persistence Loading completed successfully.')
    #
    #     except Exception as e:
    #         logger.exception(f'Error occurred during persistence loading: {e}')


if __name__ == '__main__':
    main()
