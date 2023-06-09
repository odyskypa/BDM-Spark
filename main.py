import os
import argparse
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

# Load environment variables from config..env
load_dotenv()

# Define local data directory path from environment variables
# GLOBAL_DATA_DIR_PATH = os.getenv('GLOBAL_DATA_DIR_PATH')

# Define HDFS directory paths from temporal landing zone

# TEMPORAL_LANDING_DIR_PATH = os.getenv('TEMPORAL_LANDING_DIR_PATH')
# TEMPORAL_LANDING_CSV_DIR_PATH = os.getenv('TEMPORAL_LANDING_CSV_DIR_PATH')
# TEMPORAL_LANDING_JSON_DIR_PATH = os.getenv('TEMPORAL_LANDING_JSON_DIR_PATH')

# Define API authentication key for Open Data BCN API
OPEN_DATA_API_KEY = os.getenv('OPEN_DATA_API_KEY')

# Define HDFS and HBase connection parameters from environment variables
# HDFS_HBASE_HOST = os.getenv('HDFS_HBASE_HOST')
# HDFS_PORT = os.getenv('HDFS_PORT')
# HDFS_USER = os.getenv('HDFS_USER')
# HBASE_PORT = os.getenv('HBASE_PORT')


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
                OPEN_DATA_API_KEY,
                logger)

            # Run the data collection functions
            # data_collector.upload_csv_files_to_hdfs(TEMPORAL_LANDING_CSV_DIR_PATH)
            #             # data_collector.upload_json_files_to_hdfs(TEMPORAL_LANDING_JSON_DIR_PATH)
            #             # data_collector.download_from_opendata_api_to_hdfs()

            logger.info('Data collection completed successfully.')

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
