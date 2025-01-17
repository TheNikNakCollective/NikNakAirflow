import ffmpeg
import tempfile
import niknak.utils.fs as fs
import os

def extract_key_frames(input_file: str, output_dir: str, scene_threshold: float = 0.4):
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False, prefix="converted") as input_tmp_file:
            input_tmp_file_path = input_tmp_file.name

            fs.download(input_file, input_tmp_file_path)
            
            with tempfile.TemporaryDirectory(prefix="frames") as output_tmp_dir:
                output_pattern = f"{output_tmp_dir}/frame_%04d.jpg"

                stream = ffmpeg.input(input_tmp_file_path)
                
                stream = ffmpeg.output(
                    stream,
                    output_pattern,
                    vf=f"select='gt(scene,{scene_threshold})'",
                    vsync="vfr"
                )
                
                ffmpeg.run(stream, capture_stdout=True, capture_stderr=True, overwrite_output=True)

                frames = []

                for frame in os.listdir(output_tmp_dir):
                    frame_upload_path = os.path.join(output_dir, os.path.basename(str(frame)))

                    fs.upload(os.path.join(output_tmp_dir, str(frame)), frame_upload_path)

                    frames.append(frame_upload_path)

                return frames
    except ffmpeg.Error as e:
        print("FFmpeg error occurred:")
        print('stdout:', e.stdout.decode('utf8'))
        print('stderr:', e.stderr.decode('utf8'))
        raise
