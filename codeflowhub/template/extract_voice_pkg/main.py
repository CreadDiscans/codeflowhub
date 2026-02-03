import os
import zipfile
import math
import shutil

from codeflowhub import get_storage

def extract_voice(args):
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
    import torchaudio
    import demucs.pretrained
    import demucs.separate
    if not 'extract_voice_input' in args:
        print('no input so do nothing.')
        return args
    workspace = '/tmp/input'
    output_dir = '/tmp/output'
    output_zip = '/tmp/voice.zip'
    if os.path.exists(workspace):
        shutil.rmtree(workspace)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    storage = get_storage(args['env'])
    input_path = storage.download(args['extract_voice_input'], '/tmp')

    sample_rate = args.get('extract_voice_sample_rate', 22050)
    format = args.get('extract_voice_format', 'mp3')
    stereo = args.get('extract_voice_stereo', False)

    with zipfile.ZipFile(input_path, 'r') as zipf:
        zipf.extractall(workspace)
    audio_files = []
    for root, _, files in os.walk(workspace):
        for file in files:
            audio_files.append(os.path.join(root, file))

    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    model = demucs.pretrained.get_model('htdemucs')
    model.eval()

    output = []

    CHUNK_DURATION = 300 # 5분
    cnt = len(audio_files)
    for i, audio in enumerate(audio_files):
        full_wave = demucs.separate.load_track(audio, model.audio_channels, model.samplerate)
        print('extract voice', audio, i, cnt, full_wave.size())
        duration = full_wave.size(1) / model.samplerate
        chunk_cnt = math.floor(duration / CHUNK_DURATION)
        if duration % CHUNK_DURATION > CHUNK_DURATION//2 :
            chunk_cnt += 1
        if chunk_cnt == 0:
            chunk_cnt = 1
        
        output_wav = []
        if model.samplerate != sample_rate:
            resampler = torchaudio.transforms.Resample(model.samplerate, sample_rate)
        for i in range(chunk_cnt):
            print(i, '/', chunk_cnt)
            start = i*CHUNK_DURATION
            if i+1 == chunk_cnt:
                wav = full_wave[:, start*model.samplerate:]
            else:
                end = (i+1)*CHUNK_DURATION
                wav = full_wave[:, start*model.samplerate:end*model.samplerate]

            ref = wav.mean(0)
            wav -= ref.mean()
            wav /= ref.std()
            sources = demucs.separate.apply_model(
                model, wav[None], 
                device=device, 
                num_workers=4
            )[0]
            sources *= ref.std()
            sources += ref.mean()
            
            for source, name in zip(sources, model.sources):
                if name == 'vocals':
                    if model.samplerate != sample_rate:
                        partianl_wav = resampler(source.clone())
                    else:
                        partianl_wav = source.clone()
                    if not stereo:
                        partianl_wav = torch.mean(partianl_wav, dim=0, keepdim=True)
                    output_wav.append(partianl_wav)
            del wav
            del sources

        output_wav = torch.concat(output_wav, dim=1)

        filename = '.'.join(audio.split('/')[-1].split('.')[:-1])
        path = os.path.join(output_dir, f'{filename}.{format}')
        output.append(path)
        torchaudio.save(path, output_wav, sample_rate)
    
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, output_dir)
                    zipf.write(file_path, arcname)

    s3_url = storage.upload(output_zip)
    return {
        **args,
        'extract_voice_output': s3_url
    }