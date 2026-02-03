import os
import tempfile
import tarfile
import zipfile
import subprocess
import json
import shutil
from pathlib import Path

from codeflowhub import get_storage

def download_model(env):
    import boto3
    cache_dir = Path.home() / '.cache' / 'huggingface' / 'hub'
    cache_dir.mkdir(parents=True, exist_ok=True)
    model_marker = cache_dir / '.pyannote_downloaded'
    if model_marker.exists():
        print("Pyannote model already exists in cache")
        return

    s3_client = boto3.client(
        's3',
        endpoint_url=env['ENDPOINT'],
        aws_access_key_id=env['ACCESS_KEY'],
        aws_secret_access_key=env['SECRET_KEY']
    )
    with tempfile.NamedTemporaryFile(delete=False, suffix='.tar.gz') as tmp_file:
        tmp_path = tmp_file.name
    s3_client.download_file(
        Bucket='static',
        Key='models/pyannote.tar.gz',
        Filename=tmp_path
    )
    with tarfile.open(tmp_path, 'r:gz') as tar:
        tar.extractall(path=cache_dir)
    model_marker.touch()
    os.unlink(tmp_path)

def extract_zip(zip_path, workspace):
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        zipf.extractall(workspace)
    audio_files = []
    for root, _, files in os.walk(workspace):
        for file in files:
            audio_files.append(os.path.join(root, file))
    return audio_files

def analyze_speaker(args):
    '''
        demucs를 이용하여 오디오에서 vocie를 분리
        모델 캐시 폴더 : /root/.cache
        args['extract_voice_input'] ="오디오 파일을 압축한 zip파일의 s3 url" # Required
        args['extract_voice_output'] = "voice 오디오 파일을 압축한 zip파일의 s3 url" # Return
        args['extract_voice_sample_rate'] = 22050 # Optional
        args['extract_voice_format'] = "mp3" # Optional
        args['extract_voice_stereo'] = False # Optional
    '''
    import torch
    from pyannote.audio import Pipeline
    if not 'analyze_speaker_input' in args:
        print('no input so do nothing.')
        return args
    storage = get_storage(args['env'])
    input_zip = storage.download(args['analyze_speaker_input'], '/tmp')
    download_model(args['env']['MINIO'])

    input_dir = '/tmp/input'
    output_dir = '/tmp/output'
    result_path = '/tmp/speakers.json'
    if os.path.exists(input_dir):
        shutil.rmtree(input_dir)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    audio_files = extract_zip(input_zip, input_dir)

    pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization-community-1")
    pipeline.to(torch.device("cuda"))

    results = []
    for audio_path in audio_files:
        print(f"\nAnalyzing: {audio_path}")

        # Get audio duration
        duration_cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            audio_path
        ]
        duration_result = subprocess.run(duration_cmd, capture_output=True, text=True)
        total_duration = float(duration_result.stdout.strip())

        print(f"Total duration: {total_duration:.2f} seconds")

        # Split into 10-minute (600 seconds) chunks
        chunk_duration = 600
        num_chunks = int((total_duration + chunk_duration - 1) / chunk_duration)

        audio_name = Path(audio_path).stem
        chunk_results = []

        for i in range(num_chunks):
            start_time = i * chunk_duration
            chunk_output = os.path.join(output_dir, f"{audio_name}_chunk_{i:03d}.wav")

            print(f"  Processing chunk {i+1}/{num_chunks} (start: {start_time}s)...")

            # Extract chunk and convert to WAV
            ffmpeg_cmd = [
                'ffmpeg', '-y',
                '-ss', str(start_time),
                '-t', str(chunk_duration),
                '-i', audio_path,
                '-acodec', 'pcm_s16le',  # WAV format
                '-ar', '16000',  # 16kHz sample rate
                '-ac', '1',  # Mono
                chunk_output
            ]

            subprocess.run(ffmpeg_cmd, capture_output=True, check=True)

            # Run diarization on chunk
            print(f"    Running speaker diarization...")
            diarization = pipeline(chunk_output)

            # Adjust timestamps based on chunk start time
            adjusted_segments = []
            for turn, speaker in diarization.speaker_diarization:
                adjusted_segments.append({
                    'start': turn.start + start_time,
                    'end': turn.end + start_time,
                    'speaker': speaker
                })

            chunk_results.extend(adjusted_segments)

            # Clean up chunk file
            os.remove(chunk_output)

        results.append({
            'audio': audio_path,
            'segments': chunk_results
        })

        print(f"✓ Found {len(set(s['speaker'] for s in chunk_results))} speakers")
    with open(result_path, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    s3_url = storage.upload(result_path)
    return {
        **args,
        'analyze_speaker_output': s3_url
    }
