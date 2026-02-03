from urllib.parse import urlparse, unquote
import tempfile

from  .storage import Storage

class S3Storage(Storage):

    def __init__(self, env):
        import boto3
        self.env = env
        self.bucket = env['BUCKET']

        options = {}
        def has_key(key:str):
            return key in env and env[key]

        if has_key('ACCESS_KEY_ID'):
            options['aws_access_key_id'] = env['ACCESS_KEY_ID']
        if has_key('SECRET_ACCESS_KEY'):
            options['aws_secret_access_key'] = env['SECRET_ACCESS_KEY']
        if has_key('REGION_NAME'):
            options['region_name'] = env['REGION_NAME']
        if has_key('ENDPOINT'):
            options['endpoint_url'] = env['ENDPOINT']
        self.s3 = boto3.client('s3', **options)

    def upload(self, file_path):
        filename = file_path.split('/')[-1]
        key = self.get_key(filename)
        self.s3.upload_file(file_path, self.bucket, key)
        return f's3://{self.bucket}/{key}'

    def download(self, s3_url:str, local_path):
        if s3_url.startswith('https://'):
            s3_url = self.https_to_s3_url(s3_url)
        parsed = urlparse(s3_url)
        bucket_name = parsed.netloc
        key = parsed.path.lstrip('/')
        dest = f'{local_path}/{self.get_filename(s3_url)}'
        self.s3.download_file(bucket_name, key, dest)
        return dest

    def https_to_s3_url(self, https_url: str) -> str:
        parsed = urlparse(https_url)
        netloc = parsed.netloc
        suffix = ".s3.amazonaws.com"
        if netloc.endswith(suffix):
            bucket = netloc[:-len(suffix)]
        else:
            raise ValueError(f"URL netloc does not end with expected suffix: {netloc}")
        key = parsed.path.lstrip("/")
        return f"s3://{bucket}/{key}"

    def get_filename(self, s3_url):
        parsed = urlparse(s3_url)
        bucket_name = parsed.netloc
        key = parsed.path.lstrip('/')

        response = self.s3.head_object(Bucket=bucket_name, Key=key)
        filename = None
        if 'ContentDisposition' in response:
            content_disposition = response['ContentDisposition']
            if 'filename=' in content_disposition:
                filename = content_disposition.split('filename=')[-1].strip('"\'')
        if filename is None and 'Metadata' in response and 'filename' in response['Metadata']:
            filename = response['Metadata']['filename']
        if filename is None:
            filename = key.split('/')[-1]
            
        if filename.startswith("utf-8''"):
            return unquote(filename[7:])
        else:
            return filename

    def url_to_s3(self, url, filename):
        import requests
        s3_key = self.get_key(filename)
        with requests.get(url, stream=True) as response:
            if response.status_code != 200:
                raise Exception(f'파일 다운로드 실패: {response.status_code}')
            with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        tmp_file.write(chunk)
                tmp_file.seek(0)
                self.s3.upload_fileobj(tmp_file, self.bucket, s3_key)
        return f's3://{self.bucket}/{s3_key}'