from .storage import Storage
from .local_storage import LocalStorage
from .s3_storage import S3Storage

def get_storage(env) -> Storage:
    if 'BUCKET' in env:
        return S3Storage(env)
    elif 'WORKSPACE' in env:
        return LocalStorage(env)
    else:
        raise Exception('BUCKET or WORKSPACE is required')