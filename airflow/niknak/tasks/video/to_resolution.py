from typing import Dict
from airflow.decorators import task
from airflow.models.param import Param


@task
def to_resolution(data: Dict, resolution: str):
    from niknak.utils.video.video import Video

    video_id = data.get("video_id")
    user_id = data.get("user_id")
    video_path = data.get("video_path")
    storage_provider = data.get("video_provider")

    video = Video(storage_provider, user_id, video_id, video_path)

    return {f"{resolution}p_file": video.to_resolution(resolution)}
