from airflow.decorators import task
from airflow.models.param import Param


@task(
    params={"video_id": Param("Undefined!", type="string")},
)
def to_mp4(**context):
    from typing import Dict
    from niknak.utils.video.video import Video

    params: Dict = context["params"]
    video_id = params.get("video_id")
    user_id = params.get("user_id")
    video_path = params.get("video_path")
    storage_provider = params.get("video_provider")

    video = Video(storage_provider, user_id, video_id, video_path)

    return {
        **params,
        "converted_file": video.to_mp4()
    }
