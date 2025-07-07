import psycopg2
from psycopg2.extensions import cursor as Cursor
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from scripts.config import Config


# in process
def execute(ti):
    try:
        config = Config()
        utc_timestamp = ti.xcom_pull(task_ids="clean_csv")

        print("==== connecting to redshift ====")
        conn = psycopg2.connect(
            host=config.RS_END_POINT,
            port=5439,
            dbname=config.RS_DB_NAME,
            user=config.RS_USER_NAME,
            password=config.RS_PASSWORD,
        )
        print("==== connected to redshift ====")

        cur = conn.cursor()
        s3_to_staging_table(cur, config, utc_timestamp)
        staging_to_obt(cur, config)

        print("==== commit command ====")
        conn.commit()
        print("==== duplicated data is removed ====")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        raise (e)


def s3_to_staging_table(cur: Cursor, config: Config, utc_timestamp: str):
    print("==== transfer csv from s3 to redshift staging ====")
    cur.execute(f"""
            COPY {config.RS_STAGING_OBT}
            FROM '{config.get_s3_url_path(utc_timestamp)}'
            IAM_ROLE '{config.RS_IAM_ROLE}'
            FORMAT AS CSV
            IGNOREHEADER 1;
        """)
    
def staging_to_obt(cur: Cursor, config: Config):
    print("==== staging to one big tale ====")
    cur.execute(f"""
            INSERT INTO {config.RS_OBT}
            SELECT * FROM {config.RS_STAGING_OBT}
            WHERE article_id not in (SELECT article_id FROM {config.RS_OBT});

            DELETE FROM {config.RS_STAGING_OBT};
        """)
