from typing import Dict
from airflow.decorators import task
from airflow.models.param import Param


@task
def to_resolution(data: Dict, resolution: str):
    from typing import Dict
    from niknak.utils.video.to_resolution import to_resolution
    import niknak.utils.fs as fs
    import niknak.env as env

    video_id = data.get("video_id")
    user_id = data.get("user_id")
    video_provider = data.get("video_provider")
    converted_file = data.get("converted_file")

    output_file = f"{video_provider}://{env.NIKNAK_RAW_DATA_FOLDER}/user-uploads/videos/{user_id}/{video_id}/{resolution}.mp4"

    return {
        **data,
        f"{resolution}p_file": to_resolution(
            converted_file,
            output_file,
            resolution
        )
    }
