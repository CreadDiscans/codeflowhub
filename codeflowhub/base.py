import json
import os
from .action import Action

class BaseDecorator:
    name: str = None
    func = None
    depend: list['BaseDecorator'] = None
    run_log_file: str = 'run.json'  # 클래스 변수 (모든 인스턴스 공유)

    def __init__(self, *args, **kwargs):
        self.func = None
        self.depend = []
        if args and callable(args[0]):
            self._initialize_with_func(args[0])
        elif 'name' in kwargs:
            self.name = kwargs['name']

    def _initialize_with_func(self, func):
        """함수로 초기화"""
        self.func = func
        self.name = func.__name__
        self.init()

    def __call__(self, *args, **kwargs):
        if self.func is None and args and callable(args[0]):
            self._initialize_with_func(args[0])
            return self

        if args and isinstance(args[0], Action):
            return self._handle_action(args[0], *args[1:], **kwargs)

        return self._wrapper_func(*args, **kwargs) if self.func else self

    def _handle_action(self, action, *args, **kwargs):
        """Action을 처리하여 의존성 구성"""
        if action.type == 'build':
            self.depend = [arg for arg in args if isinstance(arg, BaseDecorator)]
            print(f'Task {self.name} depends on: {[d.name for d in self.depend]}')
        return self

    def init(self):
        """서브클래스에서 오버라이드"""
        pass

    def _wrapper_func(self, *args, **kwargs):
        """함수 실행 및 결과 로깅"""
        input_data = args[0] if args else {}
        result = self.func(*args, **kwargs)

        if not isinstance(result, dict):
            raise Exception(f'result must be json serializable, got {type(result)}: {result}')

        self._save_run_log(input_data, result)
        return result

    def _load_log_file(self):
        """로그 파일 로드"""
        if not os.path.exists(self.run_log_file):
            return {}
        with open(self.run_log_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _write_log_file(self, data):
        """로그 파일 저장"""
        with open(self.run_log_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _save_run_log(self, input_data, output_data):
        """task의 input/output을 run.json에 저장"""
        run_log = self._load_log_file()
        run_log[self.name] = {'input': input_data, 'output': output_data}
        self._write_log_file(run_log)
