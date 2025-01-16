from airflow.decorators import dag
from datetime import datetime

@dag(
    start_date=datetime(2023, 2, 8),
    schedule=None,
    catchup=False,
)
def process_video():
    from niknak.tasks.video.extract import extract
    from airflow.models.xcom_arg import XComArg

    extract()


dag = process_video()