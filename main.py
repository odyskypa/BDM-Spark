import os
import argparse
import findspark
import logging.handlers
from dotenv import load_dotenv
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
            data_formatter = DataFormatter(logger, VM_HOST, MONGODB_PORT, PERSISTENT_DB, FORMATTED_DB)
            # data_formatter.format_lookup_table("lookup_table_district",
            #                                    "income_lookup_district", "rent_lookup_district")
            # data_formatter.format_lookup_table("lookup_table_neighborhood",
            #                                    "income_lookup_neighborhood", "rent_lookup_neighborhood")
            data_formatter.reconcile_data_with_lookup("persistent.income", "formatted.lookup_table_district",
                                                      "formatted.income_reconciled", "district_name",
                                                      "district_reconciled", "_id", "district_id")

            data_formatter.reconcile_data_with_lookup("persistent.building_age", "formatted.lookup_table_district",
                                                      "formatted.building_age_reconciled", "district_name",
                                                      "district_reconciled", "_id", "district_id")

            logger.info('Building the Formatted Zone from the Persistent Zone completed successfully')

        except Exception as e:

            logger.exception(f'Error occurred during data formatting process: {e}')

    # elif exec_mode == 'another-arg':
    #     try:
    #
    #         logger.info('another-arg run completed successfully.')
    #
    #     except Exception as e:
    #         logger.exception(f'Error occurred during .....: {e}')


if __name__ == '__main__':
    main()
