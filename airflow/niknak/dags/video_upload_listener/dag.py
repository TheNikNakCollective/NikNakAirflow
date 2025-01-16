from typing import Dict
from airflow.decorators import dag
from datetime import datetime
import json
from airflow.providers.apache.kafka.sensors.kafka import (
    AwaitMessageTriggerFunctionSensor,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from confluent_kafka import Message

KAFKA_TOPIC = "video.upload"

def consume_function(message: Message):
    "Takes in consumed messages and prints its contents to the logs."

    print(message)

    return json.loads(message.value())

def event_triggered_function(event, **context):
    print("event")
    print(event)
    TriggerDagRunOperator(
        trigger_dag_id="video_upload_consumer",
        task_id=f"to_mp4",
        wait_for_completion=True,
        conf=event,
        poke_interval=5,
    ).execute(context)

@dag(
    start_date=datetime(2023, 2, 8),
    catchup=False,
    tags=["kafka:consumer"],
    schedule="@continuous",
    max_active_runs=1,
)
def video_upload_listener():
    AwaitMessageTriggerFunctionSensor(
        task_id="listen_for_video_upload",
        kafka_config_id="kafka_listener",
        topics=[KAFKA_TOPIC],
        # the apply function will be used from within the triggerer, this is
        # why it needs to be a dot notation string
        apply_function="niknak.dags.video_upload_listener.dag.consume_function",
        poll_interval=5,
        poll_timeout=1,
        apply_function_kwargs={},
        event_triggered_function=event_triggered_function,
    )


dag = video_upload_listener()