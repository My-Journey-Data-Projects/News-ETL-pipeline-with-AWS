import requests
from ..data_models import ResponseModel, ResultList
from datetime import datetime
import json
from ..s3_bucket_manager import S3BucketManager
from datetime import datetime, timezone
from scripts.config import Config

# Need refactoring
def get_data(page_id: str = None):
    config = Config()
    apikey_payload=f'apikey={config.NEWS_API_KEY}'
    language_payload=f'language={config.NEWS_LANGUAGE}'
    page_payload = f'&page={page_id}' if page_id is not None else ''

    path = f"https://newsdata.io/api/1/latest?{apikey_payload}&{language_payload}{page_payload}"

    print(f"fetching from: {path}")
    try:
        res = requests.get(path)
    except:
        raise ConnectionError("Cannot connect to data source")
    return res.json()

def execute():
    try: 
        result_list: ResultList = ResultList()
        
        number_of_pages = 5 # need change later
        next_page_id = None

        print("==== fetching data from NEW.IO ====")
        for i in range(1, number_of_pages + 1):
            res_obj = get_data(next_page_id)
            res = ResponseModel(**res_obj)

            next_page_id = res.nextPage
            for result in res.results:
                result_list.data.append(result)

        print("==== dumps to json ====")
        content_json = json.dumps(result_list.data, indent=4)

        print("==== upload to S3 Bucket ====")
        config = Config()
        S3BucketManager.connect_to_s3(
            config.S3_BUCKET_NAME, 
            config.get_s3_connection_params())
        
        utc_timestamp = datetime.now(timezone.utc).timestamp()
        S3BucketManager.upload(content=content_json, path=config.get_s3_raw_json_key(utc_timestamp))
        print("==== complete ====")

        return utc_timestamp
   
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        raise(e)