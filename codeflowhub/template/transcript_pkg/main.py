import os
import zipfile
import json
import shutil
import subprocess

from codeflowhub import get_storage

def extract_zip(zip_path, workspace):
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        zipf.extractall(workspace)
    audio_files = []
    for root, _, files in os.walk(workspace):
        for file in files:
            audio_files.append(os.path.join(root, file))
    return audio_files

def preprocess_audio(input_path):
    denoised_path = os.path.join('/tmp', "denoised.wav")
    cmd = [
        "ffmpeg", "-y", "-i", input_path,
        "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
        "-ac", "1",
        "-ar", "16000",
        denoised_path
    ]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return denoised_path


def remove_long_silence(audio_path, silence_threshold=-30, min_silence_len=3000):
    """
    오디오 파일에서 3초 이상의 무음 구간을 모두 제거하고 매핑 정보를 반환합니다.

    Args:
        audio_path: 원본 오디오 파일 경로
        silence_threshold: 무음으로 간주할 데시벨 임계값 (기본: -30 dB)
        min_silence_len: 제거할 최소 무음 길이 (밀리초, 기본: 3000ms = 3초)

    Returns:
        tuple: (처리된 오디오 파일 경로, 시간 매핑 리스트)
        시간 매핑 리스트: [{'original_start': 0, 'original_end': 100, 'new_start': 0, 'new_end': 100}, ...]
    """
    from pydub import AudioSegment
    from pydub.silence import detect_nonsilent

    audio = AudioSegment.from_file(audio_path)

    # 무음이 아닌 구간 감지 (밀리초 단위)
    nonsilent_ranges = detect_nonsilent(
        audio,
        min_silence_len=min_silence_len,
        silence_thresh=silence_threshold
    )

    if not nonsilent_ranges:
        # 전체가 무음인 경우
        return audio_path, []

    # 무음이 아닌 구간들만 추출하여 연결
    audio_segments = []
    time_mapping = []
    new_position = 0

    for start_ms, end_ms in nonsilent_ranges:
        segment = audio[start_ms:end_ms]
        audio_segments.append(segment)

        # 시간 매핑 정보 저장
        segment_duration = end_ms - start_ms
        time_mapping.append({
            'original_start': start_ms / 1000.0,  # 초 단위
            'original_end': end_ms / 1000.0,
            'new_start': new_position / 1000.0,
            'new_end': (new_position + segment_duration) / 1000.0
        })

        new_position += segment_duration

    # 무음 구간이 제거되었는지 확인
    if len(nonsilent_ranges) == 1 and nonsilent_ranges[0][0] == 0 and nonsilent_ranges[0][1] == len(audio):
        # 무음 구간이 없는 경우 원본 반환
        return audio_path, [{'original_start': 0, 'original_end': len(audio) / 1000.0, 'new_start': 0, 'new_end': len(audio) / 1000.0}]

    # 무음 제거된 오디오 생성
    combined_audio = sum(audio_segments)

    # 처리된 파일 저장
    output_path = audio_path.rsplit('.', 1)[0] + '_trimmed.' + audio_path.rsplit('.', 1)[1]
    combined_audio.export(output_path, format=audio_path.split('.')[-1])

    return output_path, time_mapping

def transcript(args):
    import whisper
    if not 'transcript_input' in args:
        print('no input so do nothing.')
        return args
    storage = get_storage(args['env'])
    input_zip = storage.download(args['transcript_input'], '/tmp')

    input_dir = '/tmp/input'
    output_path = '/tmp/output.json'
    if os.path.exists(input_dir):
        shutil.rmtree(input_dir)
    os.makedirs(input_dir, exist_ok=True)
    audio_files = extract_zip(input_zip, input_dir)

    model = whisper.load_model('large-v3')
    result = {}
    for i, audio in enumerate(audio_files):
        print('transcription', audio, f'{i}/{len(audio_files)}')
        filename = audio.split('/')[-1]
        result[filename] = []
        denoised_wav = preprocess_audio(audio)
        trimmed_audio, time_mapping = remove_long_silence(
            denoised_wav, silence_threshold=-30, min_silence_len=3000)
        res = model.transcribe(trimmed_audio)

        def map_to_original_time(trimmed_time):
            for mapping in time_mapping:
                if mapping['new_start'] <= trimmed_time <= mapping['new_end']:
                    offset = trimmed_time - mapping['new_start']
                    return mapping['original_start'] + offset
            return trimmed_time

        result[filename] = {
            'text': res['text'],
            'segments': [{
                'start': map_to_original_time(seg['start']), 
                'end': map_to_original_time(seg['end']), 
                'text': seg['text']
            } for seg in res['segments']],
        }

    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    s3_url = storage.upload(output_path)
    return {
        **args,
        'transcript_output': s3_url
    }