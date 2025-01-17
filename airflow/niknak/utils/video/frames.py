from typing import List
import niknak.env as env
import niknak.utils.fs as fs
import tempfile

class Frames():
    def __init__(self, storage_provider: str, user_id: int, video_id: str):
        self.storage_provider = storage_provider
        self.user_id = user_id
        self.video_id = video_id

    def contents_url(self):
        return f"{self.storage_provider}://{env.NIKNAK_RAW_DATA_FOLDER}/frames/videos/{self.user_id}/{self.video_id}/frames/contents.txt"
    
    def save(self, frames: List[str]):
        frames_file_path = self.contents_url()
        
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, prefix="contents") as input_tmp_file:
            input_tmp_file_path = input_tmp_file.name

            with open(input_tmp_file_path, "w") as file:
                contents = "\n".join(frames)
                file.write(contents)

            fs.upload(input_tmp_file_path, frames_file_path)

        return frames_file_path
    
    def get(self):
        frames_file_path = self.contents_url()

        with fs.read(frames_file_path, "r") as file:
            content: str = file.read()

            return content.split("\n")