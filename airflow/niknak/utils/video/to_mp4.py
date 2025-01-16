import ffmpeg
import tempfile
import niknak.utils.fs as fs

def to_mp4(input_file: str, output_file: str):
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False, prefix="converted") as input_tmp_file:
            input_tmp_file_path = input_tmp_file.name

            fs.download(input_file, input_tmp_file_path)
            
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_file:
                output_tmp_file_path = tmp_file.name

                stream = ffmpeg.input(input_tmp_file_path)
                stream = ffmpeg.output(stream, output_tmp_file_path, vcodec="libx264", acodec="aac")

                ffmpeg.run(stream, capture_stdout=True, capture_stderr=True, overwrite_output=True)

                fs.upload(output_tmp_file_path, output_file)

                return output_file
    except ffmpeg.Error as e:
        print("FFmpeg error occurred:")
        print('stdout:', e.stdout.decode('utf8'))
        print('stderr:', e.stderr.decode('utf8'))
        raise
