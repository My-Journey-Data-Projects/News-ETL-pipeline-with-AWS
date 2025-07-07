import sys
import os
from io import StringIO
import spacy
import ast

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from scripts.s3_bucket_manager import S3BucketManager
import pandas as pd
from scripts.config import Config

def execute(ti):
    try:
        config = Config()
        S3BucketManager.connect_to_s3(
            bucket_name=config.S3_BUCKET_NAME,
            params=config.get_s3_connection_params())

        utc_timestamp = ti.xcom_pull(task_ids="convert_to_csv")
        obj = S3BucketManager.load(path_to_file=config.get_s3_raw_csv_key(utc_timestamp))
        body = obj.get()["Body"].read()

        df = pd.read_csv(StringIO(body.decode("utf-8")))
        df = remove_unwanted_columns(df)
        df = capitalize_source_attributes(df)
        df = extract_creators(df)
        df = clean_country_col(df)
        df['pubDate'] = pd.to_datetime(df['pubDate'])
        df.rename(columns={'pubDate': 'pub_date'}) # make it matches other columns


        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        S3BucketManager.upload(
            content=csv_buffer.getvalue(),
            path=config.get_s3_cleansed_csv_key(utc_timestamp),
        )

        return utc_timestamp

    except Exception as e:
        raise (e)


def remove_unwanted_columns(df):
    # remove unneccessary columns
    df = df.drop(
        [
            "content",
            "video_url",
            "sentiment",
            "sentiment_stats",
            "ai_tag",
            "ai_region",
            "ai_org",
            "duplicate",
            "language",
            "image_url",
            "keywords",
            "pubDateTZ",
            "source_icon"
        ],
        axis=1,
    )
    return df


def capitalize_source_attributes(df):
    df[["source_id", "source_name"]] = df[["source_id", "source_name"]].apply(
        lambda x: x.str.upper()
    )
    return df


def clean_country_col(df):
    df["country"] = df["country"].apply(
        lambda x: ast.literal_eval(x) if pd.notnull(x) else []
    )
    df["country"] = df["country"].apply(
        lambda x: x[0] if isinstance(x, list) and len(x) == 1 else []
    )

    return df

def extract_creators(df):
    nlp = spacy.load("en_core_web_sm")
    new_creator = []

    for creator in df["creator"]:
        try:
            name = ast.literal_eval(creator)
        except (ValueError, SyntaxError):
            new_creator.append([])
            continue

        creators = []
        for doc in nlp.pipe(name):
            for ent in doc.ents:
                if ent.label_ == "PERSON":
                    creators.append(ent.text)
        new_creator.append(creators)

    df["creator"] = new_creator

    return df
