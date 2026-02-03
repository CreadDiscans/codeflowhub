import os
import shutil

from  .storage import Storage

class LocalStorage(Storage):

    def __init__(self, env):
        super().__init__(env)
        self.workspace = env['WORKSPACE']

    def upload(self, file_path):
        filename = file_path.split('/')[-1]
        key = f"{self.workspace}/{self.get_key(filename)}"
        os.makedirs(os.path.dirname(key), exist_ok=True)
        key = shutil.copy2(file_path, key)
        return f's3://workspace/{key}'
    
    def download(self, s3_url, local_path):
        key = s3_url.replace('s3://workspace/', '')
        filename = s3_url.split('/')[-1]
        dest = f'{local_path}/{filename}'
        shutil.copy2(key, dest)
        return dest