from typing import Dict
from airflow.decorators import task


@task
def extract_key_frames(data: Dict):
    from niknak.utils.video.video import Video

    video_id = data.get("video_id")
    user_id = data.get("user_id")
    video_path = data.get("video_path")
    storage_provider = data.get("video_provider")

    video = Video(storage_provider, user_id, video_id, video_path)

    frames = video.extract_key_frames()

    return {**data, "frames": frames }
