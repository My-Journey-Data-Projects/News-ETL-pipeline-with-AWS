from .data_models import S3ConnectionParams

class Config:
    def __init__(self):
        #AWS
        self.AWS_ACCESS_KEY_ID: str = '*YOUR_ACCESS_KEY*'
        self.AWS_SECRET_ACCESS_KEY: str = 'YOUR_SECRET_KEY'
        self.AWS_REGION_NAME: str = 'AWS_REGION'

        #S3
        # THESE DIRECTORIES NEED TO BE CREATED IN S3 BEFORE RUNNING DAG
        self.S3_BUCKET_NAME: str = 'news-data-project-demo'
        self.__S3_RAW_JSON_KEY: str = 'raw-data-json/output_'
        self.__S3_RAW_CSV_KEY: str = 'raw-data-csv/output_'
        self.__S3_CLEANSED_CSV_KEY: str = 'cleansed-data-csv/output_'
        self.__S3_CLEASED_URL_PREFIX: str = 's3://news-data-project-demo/cleansed-data-csv/output_'
        # ============================================================

        #Redshift
        self.RS_END_POINT: str = 'YOUR_RED_SHIFT_END_POINT'
        self.RS_DB_NAME: str = 'YOUR_RED_SHIFT_DB'
        self.RS_USER_NAME: str = 'USER_NAME'
        self.RS_PASSWORD: str = 'PASSWORD'
        self.RS_IAM_ROLE: str = 'ROLE_THAT_HAVE_ACCESS_TO_S3'
        
        #Tables in redshift
        self.RS_STAGING_OBT: str = 'STAGING_TABLE_NAME'
        self.RS_OBT: str = 'OBT_NAME'

        #NEW_IO CONFIG
        self.NEWS_API_KEY: str = 'API_KEY_FOR_NEW_IO'
        self.NEWS_LANGUAGE: str = 'en'

        
    def get_s3_raw_json_key(self, utc_timestamp): 
        return f'{self.__S3_RAW_JSON_KEY}{utc_timestamp}.json' 
    
    def get_s3_raw_csv_key(self, utc_timestamp):
        return f'{self.__S3_RAW_CSV_KEY}{utc_timestamp}.csv'
    
    def get_s3_cleansed_csv_key(self, utc_timestamp):
        return f'{self.__S3_CLEANSED_CSV_KEY}{utc_timestamp}.csv'
    
    def get_s3_url_path(self, utc_timestamp):
        return f'{self.__S3_CLEASED_URL_PREFIX}{utc_timestamp}.csv'
    
    def get_s3_connection_params(self) -> S3ConnectionParams:
        s3_connection_params = S3ConnectionParams(
            access_id=self.AWS_ACCESS_KEY_ID,
            secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            region_name=self.AWS_REGION_NAME
        )
        return s3_connection_params
    