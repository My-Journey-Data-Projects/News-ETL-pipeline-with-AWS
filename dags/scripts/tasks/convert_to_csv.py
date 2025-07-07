from ..s3_bucket_manager import S3BucketManager
import pandas as pd
from io import StringIO
import json
from scripts.config import Config

def execute(ti):
    try:
        config = Config()
        
        S3BucketManager.connect_to_s3(
            config.S3_BUCKET_NAME, 
            config.get_s3_connection_params()
        )

        utc_timestamp = ti.xcom_pull(task_ids='fetch_data_news_io')

        obj = S3BucketManager.load(config.get_s3_raw_json_key(utc_timestamp))
        body = obj.get()['Body'].read()
        data = json.loads(body)
    
        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Convert DataFrame to CSV in-memory { it's fine with <100 MB file }
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
    
        # Upload CSV back to S3 (no local file)
        S3BucketManager.upload(content=csv_buffer.getvalue(), path=config.get_s3_raw_csv_key(utc_timestamp))

        print(df.head(10))

        return utc_timestamp
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        raise(e)