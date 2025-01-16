from airflow.decorators import task
from airflow.models.param import Param


@task(
    params={"video_id": Param("Undefined!", type="string")},
)
def to_mp4(**context):
    from typing import Dict
    from niknak.utils.video.to_mp4 import to_mp4
    import niknak.utils.fs as fs
    import niknak.env as env

    params: Dict = context["params"]
    video_id = params.get("video_id")
    user_id = params.get("user_id")
    video_path = params.get("video_path")
    video_provider = params.get("video_provider")

    input_file = f"{video_provider}://{video_path}"
    output_file = f"{video_provider}://{env.NIKNAK_RAW_DATA_FOLDER}/user-uploads/videos/{user_id}/{video_id}/converted.mp4"

    return {
        **params,
        "converted_file": to_mp4(
            input_file,
            output_file,
        )
    }
