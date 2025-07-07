import psycopg2
from psycopg2.extensions import cursor as Cursor
import sys
import os
from io import StringIO
import pandas as pd
import ast

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from scripts.config import Config
from scripts.s3_bucket_manager import S3BucketManager


def execute(ti):
    try:
        config = Config()
        conn = psycopg2.connect(
            host=config.RS_END_POINT,
            port=5439,
            dbname=config.RS_DB_NAME,
            user=config.RS_USER_NAME,
            password=config.RS_PASSWORD,
        )
        cur = conn.cursor()

        # extract each table from dataframe
        S3BucketManager.connect_to_s3(
            config.S3_BUCKET_NAME,
            config.get_s3_connection_params()
        )
        print(f"connected to S3 Bucket : {config.S3_BUCKET_NAME}")

        obj = S3BucketManager.load(
            config.get_s3_cleansed_csv_key(ti.xcom_pull(task_ids="clean_csv"))
        )
        body = obj.get()["Body"].read()
        df = pd.read_csv(StringIO(body.decode("utf-8")))
        
        print("==== creating dim_category ==== ")
        create_dim_category(cur, df)
        
        print("==== creating dim_creator ====")
        create_dim_creator(cur, df)

        print("==== creating dim_source ====")
        create_dim_source(cur, df)
        
        print("==== creating dim_country ====")
        country_id_map = create_dim_country(cur, df)
        
        print("==== creating fact_article ====")
        create_fact_article(cur, df, country_id_map)

        print("==== load to Redshift ====")
        conn.commit()
        cur.close()
        conn.close()
        print("==== tasks have been finished ====")
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        raise(e)


def create_dim_category(cur: Cursor, df: pd.DataFrame):
    category_id_map = {}

    # Step 1: Convert stringified list to actual Python list
    df["category"] = df["category"].apply(
        lambda x: ast.literal_eval(x) if pd.notnull(x) and isinstance(x, str) else []
    )

    # Step 2: Explode to get unique categories
    unique_categories = df["category"].explode().dropna().unique()

    # Step 3: Insert categories into dim_category (with dedup check)
    for cat in unique_categories:
        # Skip empty strings or invalid entries
        if not isinstance(cat, str) or not cat.strip():
            continue

        # Insert if not exists
        cur.execute(
            """
            INSERT INTO dim_category (category_name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_category WHERE category_name = %s
            )
            """,
            (cat, cat),
        )

        # Try to fetch inserted ID
        cur.execute(
            "SELECT category_id FROM dim_category WHERE category_name = %s", (cat,)
        )
        category_id = cur.fetchone()[0]
        category_id_map[cat] = category_id

    # Step 4: Insert into bridge table
    for _, row in df.iterrows():
        article_id = row["article_id"]
        categories = row["category"]

        for cat in categories:
            category_id = category_id_map.get(cat)
            if category_id:
                # Redshift-compatible version (ON CONFLICT not supported)
                cur.execute(
                    """
                    INSERT INTO bridge_to_category (article_id, category_id)
                    SELECT %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM bridge_to_category
                        WHERE article_id = %s AND category_id = %s
                    )
                    """,
                    (article_id, category_id, article_id, category_id),
                )

    return category_id_map  # Optional: return mapping for later use


def create_dim_creator(cur: Cursor, df: pd.DataFrame):
    creator_id_map = {}

    df["creator"] = df["creator"].apply(
        lambda x: ast.literal_eval(x) if pd.notnull(x) and isinstance(x, str) else []
    )

    # Step 2: Explode to get unique creator
    unique_creator = df["creator"].explode().dropna().unique()

    for creator in unique_creator:
        # Skip empty strings or invalid entries
        if not isinstance(creator, str) or not creator.strip():
            continue

        cur.execute(
            """
            INSERT INTO dim_creator (creator_name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_creator WHERE creator_name = %s
            )              
            """,
            (creator, creator),
        )

        # Try to fetch inserted ID
        cur.execute("SELECT creator_id FROM dim_creator WHERE creator_name = %s", (creator,))
        creator_id = cur.fetchone()[0]
        creator_id_map[creator] = creator_id


    # Step 4: Insert into bridge table
    for _, row in df.iterrows():
        article_id = row["article_id"]
        creators = row["creator"]

        for creator in creators:
            creator_id = creator_id_map.get(creator)
            if creator_id:
                # Redshift-compatible version (ON CONFLICT not supported)
                cur.execute(
                    """
                    INSERT INTO bridge_to_creator (article_id, creator_id)
                    SELECT %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM bridge_to_creator
                        WHERE article_id = %s AND creator_id = %s
                    )
                    """,
                    (article_id, creator_id, article_id, creator_id),
                )

    return creator_id_map  # Optional: return mapping for later use


def create_dim_country(cur: Cursor, df: pd.DataFrame):
    # country
    country_id_map = {}

    for country in df["country"]:
        cur.execute(
            """
            INSERT INTO dim_country (country_name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_country WHERE country_name = %s
            )
        """,
            (country, country),
        )

        # Try to fetch inserted ID
        cur.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (country,))
        country_id = cur.fetchone()[0]
        country_id_map[country] = country_id

    return country_id_map


def create_dim_source(cur: Cursor, df: pd.DataFrame):
    for _, row in df.iterrows():
        source_id = row["source_id"]
        source_name = row["source_name"]
        source_priority = row["source_priority"]
        source_url = row["source_url"]
        cur.execute(
            """
            INSERT INTO dim_source (source_id, source_name, source_url, source_priority)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_source WHERE source_id = %s
            )
            """,(
                source_id, source_name, source_url, source_priority, source_id
            )
        )


def create_fact_article(cur: Cursor, df: pd.DataFrame, country_id_map):
    for _, row in df.iterrows():
        article_id = row['article_id']
        title = row['title']
        link = row['link']
        description = row['description']
        pub_date = row['pubDate']
        country_id = country_id_map[row['country']]
        source_id = row['source_id']
        cur.execute(
            """
            INSERT INTO fact_article (article_id, title, link, description, country_id, source_id, published_at)
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_article WHERE article_id = %s
            )
            """,(
                article_id, title, link, description, country_id, source_id, pub_date, article_id
            )
        )
