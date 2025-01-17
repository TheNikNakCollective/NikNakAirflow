from typing import Literal
import niknak.env as env
from niknak.utils.video.extract_key_frames import extract_key_frames
from niknak.utils.video.frames import Frames
from niknak.utils.video.to_mp4 import to_mp4
from niknak.utils.video.to_resolution import to_resolution


class Video:
    def __init__(
        self,
        storage_provider: str,
        user_id: int,
        video_id: str,
        original_video_path: str,
    ):
        self.storage_provider = storage_provider
        self.user_id = user_id
        self.video_id = video_id
        self.original_video_path = original_video_path
        self.frames = Frames(storage_provider, user_id, video_id)

    def to_mp4(self):
        input_file = f"{self.storage_provider}://{self.original_video_path}"
        output_file = self.converted_filepath()

        return to_mp4(
            input_file,
            output_file,
        )

    def converted_filepath(self):
        return f"{self.storage_provider}://{env.NIKNAK_RAW_DATA_FOLDER}/user-uploads/videos/{self.user_id}/{self.video_id}/converted.mp4"

    def resolution_filepath(self, resolution: Literal["1080", "720", "480"]):
        return f"{self.storage_provider}://{env.NIKNAK_CDN_DATA_FOLDER}/videos/resolutions/{self.video_id}/{resolution}.mp4"

    def to_resolution(self, resolution: Literal["1080", "720", "480"]):
        output_file = self.resolution_filepath(resolution)

        return to_resolution(self.converted_filepath(), output_file, resolution)

    def extract_key_frames(self):
        output_dir = f"{self.storage_provider}://{env.NIKNAK_RAW_DATA_FOLDER}/frames/videos/{self.user_id}/{self.video_id}/frames"

        extracted_key_frames = extract_key_frames(
            self.resolution_filepath("480"), output_dir, 0.4
        )

        return self.frames.save(extracted_key_frames)

    def get_frames(self):
        return self.frames.get()
