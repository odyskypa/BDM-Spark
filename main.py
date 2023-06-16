import os
import argparse
import findspark
import logging.handlers
from dotenv import load_dotenv
from pyspark.sql.types import *
from pyspark.sql.functions import lit

from src.data_formatters.data_formatter import DataFormatter
from src.descriptive_analysis.data_description import DataDescription
from src.predictive_analysis.data_modeling import DataModeling
from src.utils.mongo_utils import MongoDBUtils

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
EXPLOITATION_DB = os.getenv('EXPLOITATION_DB')


def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description='Formatted and Exploitation Landing Zones')

    # Add argument for execution mode
    parser.add_argument('exec_mode', type=str, choices=['data-formatting', 'data-prediction', 'data-description'],
                        help='Execution mode')



    # Parse command line arguments
    args = parser.parse_args()
    exec_mode = args.exec_mode

    if exec_mode == 'data-formatting':

        # Add argument for action within data-formatting mode
        parser.add_argument('action', type=str, choices=['merge-lookup-tables', 'fix-data-types', 'drop-duplicates',
                                                         'reconcile-data'],
                            help='Action within data-formatting mode')
        action = args.action

        try:
            # Initialize a DataCollector instance
            data_formatter = DataFormatter(logger, VM_HOST, MONGODB_PORT, PERSISTENT_DB, FORMATTED_DB)

            if action == 'merge-lookup-tables':
                ### DONE !!!!!!!
                logger.info('Merging and deduplicate lookup tables...')

                data_formatter.merge_district_lookup_table("lookup_table_district",
                                                           "income_lookup_district", "rent_lookup_district")
                data_formatter.merge_neighborhood_lookup_table("lookup_table_neighborhood",
                                                               "income_lookup_neighborhood", "rent_lookup_neighborhood")

                logger.info('Lookup table merge and deduplication completed.')
                pass
            elif action == 'fix-data-types':
                ### DONE !!!!!!!!
                logger.info('Fixing data types...')

                # Define the new schema with the desired data types
                new_schema_lookup_district = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("district", StringType(), nullable=False),
                    StructField("district_name", StringType(), nullable=False),
                    StructField("district_reconciled", StringType(), nullable=False),
                    StructField("neighborhood_id", ArrayType(StringType()), nullable=False)
                ])

                # Call the function
                data_formatter.convert_collection_data_types(FORMATTED_DB, FORMATTED_DB, "lookup_table_district",
                                                             new_schema_lookup_district)

                new_schema_lookup_neighborhood = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("neighborhood", StringType(), nullable=False),
                    StructField("neighborhood_name", StringType(), nullable=False),
                    StructField("neighborhood_reconciled", StringType(), nullable=False),
                    StructField("ne", StringType(), nullable=True),
                    StructField("ne_n", StringType(), nullable=True),
                    StructField("ne_re", StringType(), nullable=True),
                ])

                # Call the function
                data_formatter.convert_collection_data_types(FORMATTED_DB, FORMATTED_DB, "lookup_table_neighborhood",
                                                             new_schema_lookup_neighborhood)

                new_schema_idealista = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("value", ArrayType(StructType([
                        StructField("address", StringType(), nullable=True),
                        StructField("bathrooms", IntegerType(), nullable=True),
                        StructField("country", StringType(), nullable=True),
                        StructField("detailedType", StructType([
                            StructField("subTypology", StringType(), nullable=True),
                            StructField("typology", StringType(), nullable=True)
                        ]), nullable=True),
                        StructField("distance", StringType(), nullable=True),
                        StructField("district", StringType(), nullable=True),
                        StructField("exterior", BooleanType(), nullable=True),
                        StructField("externalReference", StringType(), nullable=True),
                        StructField("floor", IntegerType(), nullable=True),
                        StructField("has360", BooleanType(), nullable=True),
                        StructField("has3DTour", BooleanType(), nullable=True),
                        StructField("hasLift", BooleanType(), nullable=True),
                        StructField("hasPlan", BooleanType(), nullable=True),
                        StructField("hasStaging", BooleanType(), nullable=True),
                        StructField("hasVideo", BooleanType(), nullable=True),
                        StructField("latitude", DoubleType(), nullable=True),
                        StructField("longitude", DoubleType(), nullable=True),
                        StructField("municipality", StringType(), nullable=True),
                        StructField("neighborhood", StringType(), nullable=True),
                        StructField("newDevelopment", BooleanType(), nullable=True),
                        StructField("newDevelopmentFinished", BooleanType(), nullable=True),
                        StructField("numPhotos", IntegerType(), nullable=True),
                        StructField("operation", StringType(), nullable=True),
                        StructField("parkingSpace", StructType([
                            StructField("hasParkingSpace", BooleanType(), nullable=True),
                            StructField("isParkingSpaceIncludedInPrice", BooleanType(), nullable=True),
                            StructField("parkingSpacePrice", DoubleType(), nullable=True)
                        ]), nullable=True),
                        StructField("price", DoubleType(), nullable=True),
                        StructField("priceByArea", DoubleType(), nullable=True),
                        StructField("propertyCode", StringType(), nullable=True),
                        StructField("propertyType", StringType(), nullable=True),
                        StructField("province", StringType(), nullable=True),
                        StructField("rooms", IntegerType(), nullable=True),
                        StructField("showAddress", BooleanType(), nullable=True),
                        StructField("size", DoubleType(), nullable=True),
                        StructField("status", StringType(), nullable=True),
                        StructField("suggestedTexts", StructType([
                            StructField("subtitle", StringType(), nullable=True),
                            StructField("title", StringType(), nullable=True)
                        ]), nullable=True),
                        StructField("thumbnail", StringType(), nullable=True),
                        StructField("topNewDevelopment", BooleanType(), nullable=True),
                        StructField("url", StringType(), nullable=True)
                    ])))
                ])

                data_formatter.convert_collection_data_types(PERSISTENT_DB, FORMATTED_DB, "idealista",
                                                             new_schema_idealista)

                new_schema_income = StructType([
                    StructField("_id", IntegerType(), nullable=False),
                    StructField("neigh_name", StringType(), nullable=False),
                    StructField("district_id", IntegerType(), nullable=False),
                    StructField("district_name", StringType(), nullable=False),
                    StructField("info", ArrayType(StructType([
                        StructField("year", IntegerType(), nullable=True),
                        StructField("pop", IntegerType(), nullable=True),
                        StructField("RFD", DoubleType(), nullable=True)
                    ])), nullable=True)
                ])

                data_formatter.convert_collection_data_types(PERSISTENT_DB, FORMATTED_DB, "income",
                                                             new_schema_income)

                new_schema_building_age = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("neigh_name", StringType(), nullable=False),
                    StructField("district_id", StringType(), nullable=False),
                    StructField("district_name", StringType(), nullable=False),
                    StructField("info", ArrayType(StructType([
                        StructField("year", IntegerType(), nullable=True),
                        StructField("mean_age", DoubleType(), nullable=True)
                    ])), nullable=True)
                ])

                data_formatter.convert_collection_data_types(PERSISTENT_DB, FORMATTED_DB, "building_age",
                                                             new_schema_building_age)

                logger.info('Data types conversion completed.')
                pass
            elif action == 'drop-duplicates':
                ### DONE !!!!!!!!
                logger.info('Dropping duplicates...')

                # logger.info('Read collection "income" from MongoDB.')
                # income_df = data_formatter.read_mongo_collection(FORMATTED_DB, "income")
                #
                # logger.info('Duplicates dropped for collection "income".')
                # deduplicated_income_df = data_formatter.drop_duplicates(income_df)
                #
                # logger.info('Read collection "building_age" from MongoDB.')
                # building_age_df = data_formatter.read_mongo_collection(FORMATTED_DB, "building_age")
                #
                # logger.info('Duplicates dropped for collection "building_age".')
                # deduplicated_building_age_df = data_formatter.drop_duplicates(building_age_df)
                #
                # logger.info('Writing deduplicated data back to MongoDB.')
                #
                # data_formatter.write_to_mongo_collection(FORMATTED_DB, "income", deduplicated_income_df)
                # logger.info('Deduplicated "income" data written to MongoDB.')
                #
                # data_formatter.write_to_mongo_collection(FORMATTED_DB, "building_age", deduplicated_building_age_df)
                # logger.info('Deduplicated "building_age" data written to MongoDB.')
                #
                # logger.info('Reading collection "idealista" from MongoDB.')
                #
                # df = data_formatter.transform_idealista_to_latest_info(FORMATTED_DB, "idealista")
                # data_formatter.write_to_mongo_collection(FORMATTED_DB, "idealista_cleaned", df)
                # logger.info('Deduplicated "idealista" data written to MongoDB.')
                data_formatter.drop_duplicates_action()

                logger.info('Duplicates dropped and data written to MongoDB successfully.')

                pass
            elif action == 'reconcile-data':
                ### Only reconciling Idealista is missing, and the names
                logger.info('Reconciling data with lookup tables...')
                data_formatter.reconcile_data_action()

                # logger.info(f"Reading lookup data from MongoDB...")
                # lookupDF_district = data_formatter.read_mongo_collection(FORMATTED_DB, "lookup_table_district")
                # lookupDF_district = lookupDF_district.select("district_name", "_id")
                # lookupDF_district = lookupDF_district.withColumnRenamed("district_name", "district")
                # lookupDF_district.cache()
                #
                # lookupDF_neighborhood = data_formatter.read_mongo_collection(FORMATTED_DB, "lookup_table_neighborhood")
                # lookupDF_neighborhood = lookupDF_neighborhood.select("neighborhood_name", "_id")
                # lookupDF_neighborhood = lookupDF_neighborhood.withColumnRenamed("neighborhood_name", "neighborhood")
                # lookupDF_neighborhood.cache()
                #
                # inputDF_income = data_formatter.read_mongo_collection(PERSISTENT_DB, "income")
                # inputDF_building_age = data_formatter.read_mongo_collection(PERSISTENT_DB, "building_age")
                # input_idealista = data_formatter.read_mongo_collection(FORMATTED_DB, "idealista_cleaned")
                # input_idealista = input_idealista.withColumnRenamed("district", "district_name")
                # input_idealista = input_idealista.withColumnRenamed("neighborhood", "neighborhood_name")
                # input_idealista = input_idealista.withColumn('district_id', lit(''))
                # input_idealista = input_idealista.withColumn('neighborhood_id', lit(''))
                #
                # # income_rec = data_formatter.reconcile_data_with_lookup(inputDF_income, lookupDF_district,
                # #                                 "district_name", "district", "_id", "district_id", 2)
                #
                # # final_inc_rec = data_formatter.reconcile_data_with_lookup(income_rec, lookupDF_neighborhood,
                # #                                 "neigh_name ", "neighborhood", "_id", "_id", 3)
                #
                # # building_age_rec = data_formatter.reconcile_data_with_lookup(inputDF_building_age, lookupDF_district,
                # #                                 "district_name", "district", "_id", "district_id", 2)
                #
                # # final_build_age_rec = data_formatter.reconcile_data_with_lookup(building_age_rec, lookupDF_neighborhood,
                # #                                                          "neigh_name", "neighborhood", "_id", "_id",
                # #                                                          3)
                #
                # # idealista_rec = data_formatter.reconcile_data_with_lookup(input_idealista, lookupDF_district,
                # # "district_name", "district", "_id",
                # # "district_id", 2)
                #
                # # final_idealista_rec = data_formatter.reconcile_data_with_lookup(idealista_rec, lookupDF_neighborhood,
                # # "neighborhood_name", "neighborhood", "_id",
                # # "neighborhood_id", 2)
                #
                # # data_formatter.write_to_mongo_collection(FORMATTED_DB, "income_reconciled", final_inc_rec)
                # # data_formatter.write_to_mongo_collection(FORMATTED_DB, "building_age_reconciled", final_build_age_rec)
                # # data_formatter.write_to_mongo_collection(FORMATTED_DB, "idealista_reconciled", final_idealista_rec)

                logger.info('Data reconciliation completed.')
                pass
            else:
                logger.error('Invalid action specified for data-formatting mode.')

            logger.info('Building the Formatted Zone from the Persistent Zone completed successfully')

        except Exception as e:

            logger.exception(f'Error occurred during data formatting process: {e}')

    elif exec_mode == 'data-description':
        try:

            # Initialize a dataDescription instance
            data_description = DataDescription(logger, VM_HOST, MONGODB_PORT, PERSISTENT_DB,
                                               FORMATTED_DB, EXPLOITATION_DB)

            logger.info('Data description processes completed.')

        except Exception as e:
            logger.exception(f'Error occurred during data description process: {e}')

    elif exec_mode == 'data-prediction':
        try:

            # Initialize a DataCollector instance
            data_prediction = DataModeling(logger, VM_HOST, MONGODB_PORT, PERSISTENT_DB, FORMATTED_DB, EXPLOITATION_DB)

            data_prediction.get_data_from_formatted_to_exploitation()

            logger.info('Data modeling processes completed.')

        except Exception as e:
            logger.exception(f'Error occurred during data modeling process: {e}')
    else:
        logger.error('Invalid execution mode specified.')


if __name__ == '__main__':
    main()
