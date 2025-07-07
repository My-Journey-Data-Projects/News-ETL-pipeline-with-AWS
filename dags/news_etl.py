from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
from scripts.tasks import fetch_data_news_io
from scripts.tasks import convert_to_csv
from scripts.tasks import clean_csv
from scripts.tasks import load_to_redshift_obt
from scripts.tasks import load_to_redshift_star_schema



default_args = {
    'owner': 'taeofkitava',
    'retries': 5,
    'retry_delay': timedelta(minutes=1) 
}


with DAG(
    dag_id='news_etl_v3',
    default_args=default_args,
    description='this is test'
    # start_date=datetime(2024, 1, 1),
    # schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='fetch_data_news_io',
        python_callable=fetch_data_news_io.execute
    )
    task2 = PythonOperator(
        task_id='convert_to_csv',
        python_callable=convert_to_csv.execute
    )
    task3 = PythonOperator(
        task_id='clean_csv',
        python_callable=clean_csv.execute
    )
    task4 = PythonOperator(
        task_id='load_to_obt',
        python_callable=load_to_redshift_obt.execute
    )
    task5 = PythonOperator(
        task_id="load_to_starschema",
        python_callable=load_to_redshift_star_schema.execute
    )

    task1.set_downstream(task2)
    task2.set_downstream(task3)
    task3.set_downstream(task4)
    task3.set_downstream(task5)
    