class DataFormatter:

    def __init__(self, open_data_api_key, logger):

        # self.temporal_landing_json = temporal_landing_json.replace('\\', '/')
        self.open_data_api_key = open_data_api_key
        self.logger = logger
        try:
            print('Hello world')
            # self.client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}', user=self.hdfs_user)
            # self.logger.info(f"Connection to HDFS has been established successfully.")
            # self.create_hdfs_dir(os.path.join(self.temporal_landing_dir))
        except Exception as e:
            # self.client.close()
            self.logger.exception(e)
