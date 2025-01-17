from typing import Dict
from airflow.decorators import dag
from datetime import datetime

@dag(
    start_date=datetime(2023, 2, 8),
    schedule=None,
    catchup=False,
)
def video_upload_consumer():
    from niknak.tasks.video.to_mp4 import to_mp4
    from niknak.tasks.video.to_resolution import to_resolution
    from niknak.tasks.video.extract_key_frames import extract_key_frames
    from airflow.decorators import task

    mp4 = to_mp4()

    output_1080p = to_resolution.override(task_id="to_1080p")(mp4, "1080")
    output_720p = to_resolution.override(task_id="to_720p")(mp4, "720")
    output_480p = to_resolution.override(task_id="to_480p")(mp4, "480")

    @task(task_id="combine")
    def merge_results(mp4: Dict, outputs: list):
        merged_data = {
            **mp4
        }

        for output in outputs:
            merged_data = {**merged_data, **output}

        return merged_data
    
    merged_data = merge_results(mp4, [output_1080p, output_720p, output_480p])

    extract_key_frames(merged_data)


dag = video_upload_consumer()