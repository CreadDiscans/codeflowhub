
class Storage:

    env = {}

    def __init__(self, env):
        self.env = env

    def upload(self, file_path) -> str:
        pass

    def download(self, s3_url:str, local_path:str) -> str:
        pass

    def get_key(self, filename):
        return f"{self.env['project']}/{self.env['run_id']}/{self.env['task']}/{filename}"