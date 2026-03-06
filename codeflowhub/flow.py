import argparse
import inspect
import json
import os
import sys
from pathlib import Path

from .base import BaseDecorator
from .action import Action
from .service import AirflowExporter

class FlowDecorator(BaseDecorator):

    namespace:str
    env:dict
    env_config:dict  # 여러 환경 설정을 담는 dict
    current_env:str  # 현재 선택된 환경
    flow_name:str  # flow의 이름 (name 파라미터)
    description:str  # flow의 설명
    params:dict  # Airflow DAG params
    tags:list  # Airflow DAG tags
    annotations:dict  # Kubernetes pod annotations
    service_account_name:str  # Kubernetes service account name
    volumes:list  # Kubernetes volumes
    airflow_sidecar_image:str  # Airflow XCom sidecar 이미지
    repo:str  # Git repository URL
    path:str  # Git repo 내 작업 경로
    on_failure: 'BaseDecorator' = None  # 모든 task의 기본 failure handler

    def __init__(self, *args, namespace='default', env=None, name=None, description=None, params=None,
                 tags=None, annotations=None, service_account_name=None, volumes=None,
                 airflow_sidecar_image=None, repo=None, path=None, on_failure=None, **kwargs):
        # CLI 속성 먼저 초기화 (init()에서 사용됨)
        self._cli_export = None
        self._cli_job_dir = None
        self._cli_task = None
        self._cli_input_data = None
        self._cli_output_file = None
        self._cli_run_log = None

        self.flow_name = name
        self.namespace = namespace
        self.description = description
        self.params = params or {}
        self.tags = tags or []
        self.annotations = annotations or {}
        self.service_account_name = service_account_name
        self.volumes = volumes or []
        self.airflow_sidecar_image = airflow_sidecar_image
        self.repo = repo
        self.path = path
        self.on_failure = on_failure

        self._initialize_env(env)
        self._parse_args()

        # super().__init__()는 마지막에 호출 (init()이 실행됨)
        super().__init__(*args, name=name, **kwargs)

    def _initialize_env(self, env):
        """환경 설정 초기화"""
        if env and isinstance(env, dict) and all(isinstance(v, dict) for v in env.values()):
            self.env_config = env
            self.env = env.get('default', {})
        elif env:
            self.env_config = {'default': env}
            self.env = env
        else:
            self.env_config = {}
            self.env = {}
        self.current_env = 'default'

    # 클래스 변수: 미리 파싱된 결과 저장
    _pre_parsed_args = None
    # 클래스 변수: 커스텀 CLI 인자 저장 (flowhub 기본 인자 제외)
    _custom_cli_args = {}

    @classmethod
    def _create_parser(cls, add_help=True):
        """FlowhHub CLI 파서 생성"""
        parser = argparse.ArgumentParser(description='FlowhHub Workflow', add_help=add_help)
        parser.add_argument('--env', type=str, default='default', help='Environment to use (default: default)')
        parser.add_argument('--id', type=str, default=None, help='Workflow run ID (default: run)')
        parser.add_argument('--export', type=str, default=None, help='Export to external system (airflow)')
        parser.add_argument('--job', type=str, default=None, help='Job directory path (auto-sets --input-data and --run-log)')
        parser.add_argument('--task', type=str, default=None, help='Run specific task (default: all when --job is used)')
        parser.add_argument('--input', '--input-data', type=str, default=None, dest='input_data', help='Input file or directory for task initial data')
        parser.add_argument('--output', '--output-file', type=str, default=None, dest='output_file', help='Output file path for final task result')
        parser.add_argument('--run-log', type=str, default='run.json', help='Run log file storing intermediate task results (default: run.json)')
        return parser

    @classmethod
    def get_parser(cls) -> argparse.ArgumentParser:
        """FlowhHub CLI 파서를 반환

        외부에서 커스텀 인자를 추가하고 싶을 때 사용합니다.
        파서에 직접 add_argument()로 커스텀 인자를 추가한 후 parse_args()를 호출하세요.

        Returns:
            argparse.ArgumentParser: FlowhHub 인자가 포함된 파서

        Example:
            from flowhub import get_parser, parse_args, flow, task

            # 1. 파서 가져와서 커스텀 인자 추가
            parser = get_parser()
            parser.add_argument('--my-option', type=str, default='default')
            parser.add_argument('--count', type=int, default=1)
            args = parse_args(parser)

            # 2. 커스텀 값을 사용하여 flow 정의
            @flow(name='my-workflow', env={
                'default': {
                    'WORKSPACE': './tmp',
                    'MY_OPTION': args.my_option,
                    'COUNT': args.count
                }
            })
            def main(flow_args):
                ...
        """
        return cls._create_parser()

    @classmethod
    def parse_args(cls, parser: argparse.ArgumentParser = None) -> argparse.Namespace:
        """CLI 인자를 파싱하고 결과를 캐시

        get_parser()로 커스텀 인자를 추가한 파서를 전달하거나,
        파서 없이 호출하면 기본 FlowhHub 파서를 사용합니다.

        Args:
            parser: 커스텀 인자가 추가된 파서 (optional)

        Returns:
            argparse.Namespace: 파싱된 인자들

        Example:
            from flowhub import get_parser, parse_args, flow

            parser = get_parser()
            parser.add_argument('--my-option', type=str)
            args = parse_args(parser)

            @flow(name='my-workflow', env={'default': {'MY_OPTION': args.my_option}})
            def main(flow_args):
                ...
        """
        if parser is None:
            parser = cls._create_parser()

        if len(sys.argv) <= 1:
            cls._pre_parsed_args = parser.parse_args([])
            return cls._pre_parsed_args

        args = parser.parse_args()
        cls._pre_parsed_args = args

        # flowhub 기본 인자 목록
        flowhub_args = {'env', 'id', 'export', 'job', 'task', 'input_data', 'output_file', 'run_log'}

        # 커스텀 인자 추출 (flowhub 기본 인자 제외)
        cls._custom_cli_args = {
            key: value for key, value in vars(args).items()
            if key not in flowhub_args and value is not None
        }

        return args

    def _parse_args(self):
        """Command line arguments 파싱

        parse_args()로 미리 파싱된 결과가 있으면 그것을 사용하고,
        없으면 parse_known_args()로 flowhub 인자만 파싱합니다.
        """
        # 미리 파싱된 결과가 있으면 사용
        if FlowDecorator._pre_parsed_args is not None:
            args = FlowDecorator._pre_parsed_args
        elif len(sys.argv) <= 1:
            self._cli_export = self._cli_task = self._cli_input_data = self._cli_output_file = self._cli_run_log = self._cli_job_dir = None
            return
        else:
            # parse_known_args()로 flowhub 인자만 파싱
            # 내부 파싱 시에는 add_help=False (외부 파서와 충돌 방지)
            parser = self._create_parser(add_help=False)
            args, _ = parser.parse_known_args()

        if args.env and args.env in self.env_config:
            self.select_env(args.env)
            print(f'Using environment: {args.env}')

        if args.id:
            self.env['run_id'] = args.id
            print(f'Using run_id: {args.id}')

        # --job 옵션 처리
        if args.job:
            job_path = Path(args.job)
            self._cli_job_dir = args.job
            # --task가 명시되지 않으면 'all' 사용
            self._cli_task = args.task if args.task else 'all'

            # --input-data가 명시되지 않으면 job 경로 기반
            # (all인 경우 input.json, 특정 task인 경우 None=run-log에서 로드)
            if not args.input_data:
                self._cli_input_data = str(job_path / 'input.json') if self._cli_task == 'all' else None
            else:
                self._cli_input_data = args.input_data

            # --run-log가 기본값이면 {job_path}/run.json 사용
            self._cli_run_log = args.run_log if args.run_log != 'run.json' else str(job_path / 'run.json')
            self._cli_output_file = args.output_file

            print(f'📁 Job directory: {args.job}')
            print(f'   Task: {self._cli_task}')
            print(f'   Input data: {self._cli_input_data or f"(from {self._cli_run_log})"}')
            print(f'   Run log: {self._cli_run_log}')
        else:
            self._cli_task = args.task
            self._cli_input_data = args.input_data
            self._cli_output_file = args.output_file
            self._cli_run_log = args.run_log
            self._cli_job_dir = None

        self._cli_export = args.export

        # run log 파일 경로를 BaseDecorator.run_log_file에 설정
        if self._cli_run_log:
            BaseDecorator.run_log_file = self._cli_run_log

    def select_env(self, env_name):
        """환경 선택"""
        if env_name not in self.env_config:
            available = list(self.env_config.keys())
            raise ValueError(f'환경 "{env_name}"을 찾을 수 없습니다. 사용 가능한 환경: {available}')

        self.current_env = env_name
        self.env = self.env_config[env_name]
        return self

    def _handle_export(self):
        """Export 처리"""
        if self._cli_export.lower() != 'airflow':
            print(f'❌ Unknown export target: {self._cli_export}')
            print(f'   Supported: airflow')
            os._exit(1)

        from datetime import datetime, timedelta

        flow_id = self.flow_name or self.name
        print(f'🚀 Exporting {flow_id} to Airflow DAG...')

        dag_path = self.export_airflow(
            output_path=f'dags/{flow_id}_dag.py',
            schedule_interval=None,
            start_date=datetime.now(),
            default_args={
                'owner': 'flowhub',
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            }
        )

        print(f'✅ Successfully exported to: {dag_path}')
        print(f'📋 DAG ID: {flow_id}')
        print(f'🏷️  Tags: [{self.namespace}]')
        os._exit(0)

    def _handle_task_run(self):
        """CLI에서 --task 옵션으로 특정 task 실행"""
        # --task all이면 모든 task를 순서대로 실행
        if self._cli_task == 'all':
            self._run_all_tasks()
            return

        # 입력 로드
        input_data = self._load_input_data_from_cli(allow_none=True)

        # run() 메서드 호출
        result = self.run(self._cli_task, input_data)

        # 결과 저장
        self._save_result_to_output(result, success_message=f'✅ Task "{self._cli_task}" 실행 완료')

    def _load_input_data_from_cli(self, allow_none=False):
        """CLI --input-data 옵션에서 입력 데이터 로드

        Args:
            allow_none: True면 --input-data 없을 때 None 반환, False면 에러
        """
        if not self._cli_input_data:
            if allow_none:
                return None
            else:
                print(f'❌ Error: --input-data가 필요합니다.')
                os._exit(1)

        input_path = Path(self._cli_input_data)

        if input_path.is_dir():
            return self._load_inputs_from_dir(input_path)
        elif input_path.is_file():
            return self._load_input_from_file(input_path)
        else:
            print(f'❌ Error: 입력 경로 {self._cli_input_data}를 찾을 수 없습니다.')
            os._exit(1)

    def _load_inputs_from_dir(self, input_path):
        """디렉토리에서 입력 데이터 로드"""
        print(f'📂 Loading inputs from directory: {input_path}')
        json_files = sorted(input_path.glob('*.json'))

        if not json_files:
            print(f'❌ Error: {input_path} 폴더에 JSON 파일이 없습니다.')
            os._exit(1)

        input_data = []
        for json_file in json_files:
            print(f'   - {json_file.name}')
            with open(json_file, 'r', encoding='utf-8') as f:
                input_data.append(json.load(f))

        print(f'✅ Loaded {len(input_data)} input file(s)')
        return input_data

    def _load_input_from_file(self, input_path):
        """파일에서 입력 데이터 로드"""
        print(f'📄 Loading input from file: {input_path}')
        with open(input_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _run_all_tasks(self):
        """모든 task를 flow 전체 실행"""
        # 입력 데이터 로드 (필수)
        input_data = self._load_input_data_from_cli(allow_none=False)

        print(f'🚀 Running full workflow...')
        print(f'📋 Tasks: {[t.name for t in self.depend]}')
        print()

        # flow 전체 실행 (self(data))
        result = self(input_data)

        # 결과 저장
        self._save_result_to_output(result, success_message='\n✅ Workflow completed')

    def _save_single_task_to_log(self, task_name, result):
        """개별 task 결과를 run.json에 저장"""
        run_log = self._load_log_file()
        run_log[task_name] = {
            'input': result,
            'output': result
        }
        self._write_log_file(run_log)

    def _save_result_to_output(self, result, success_message='✅ Task completed'):
        """결과를 --output-file 또는 run-log에 저장

        Args:
            result: 저장할 결과
            success_message: 성공 메시지 (기본: '✅ Task completed')
        """
        if self._cli_output_file:
            # --output-file 지정된 경우: 파일에 저장
            output_path = Path(self._cli_output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print(f'{success_message}. Output file: {self._cli_output_file}')
        else:
            # --output-file 없는 경우: run-log에 저장
            if self._cli_task and self._cli_task != 'all':
                self._save_single_task_to_log(self._cli_task, result)
            print(f'{success_message}. Results saved to run log: {BaseDecorator.run_log_file}')

    def _inject_env_to_data(self, data):
        """데이터에 env 주입 (중복 코드 방지용 헬퍼 메서드)"""
        if isinstance(data, dict):
            data = data.copy()
            if self.env is not None and 'env' not in data:
                env_with_defaults = self.env.copy()
                env_with_defaults.setdefault('project', self.flow_name or self.name)
                env_with_defaults.setdefault('run_id', 'run')
                env_with_defaults.setdefault('task', None)
                data['env'] = env_with_defaults
        return data

    def _wrapper_func(self, *args, **kwargs):
        """Flow 실행 시 args에 env를 자동으로 주입"""
        if args and isinstance(args[0], dict):
            input_data = self._inject_env_to_data(args[0])
            args = (input_data,) + args[1:]

        try:
            return super()._wrapper_func(*args, **kwargs)
        except Exception as e:
            if self.on_failure:
                import traceback
                print(f'❌ Workflow "{self.name}" failed: {e}')
                print(f'🔧 Calling failure handler: {self.on_failure.name}')
                # failure handler에 env 정보 + 에러 정보 포함된 데이터 전달
                failure_data = self._inject_env_to_data({
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc(),
                })
                return self.on_failure(failure_data)
            raise

    def init(self):
        # Action을 전달하여 flow 실행 → task들이 자동으로 의존성 수집
        action = Action(type='build')

        # Action에 호출된 task들을 추적할 리스트 추가
        action.called_tasks = []

        self.func(action)

        # flow의 depend에 실제로 호출된 task를 실행 순서로 정렬 (병렬인 경우 이름순)
        self.depend = self._sort_tasks_by_execution_order(action.called_tasks)

        # init 완료 후 CLI 모드 체크
        if self._cli_export:
            self._handle_export()
        elif self._cli_task:
            self._handle_task_run()
            os._exit(0)

    def _sort_tasks_by_execution_order(self, tasks):
        """
        Task들을 실행 순서로 정렬 (topological sort)
        같은 레벨(병렬 실행 가능)인 task들은 이름순으로 정렬
        """
        # 각 task의 레벨(depth) 계산
        def get_task_level(task):
            if not task.depend:
                return 0
            return max(get_task_level(dep) for dep in task.depend) + 1

        # task를 (level, name, task) 튜플로 변환하여 정렬
        task_with_level = [(get_task_level(task), task.name, task) for task in tasks]
        task_with_level.sort(key=lambda x: (x[0], x[1]))  # level 우선, 같으면 name 순

        return [task for _, _, task in task_with_level]

    def run(self, task_name, data=None):
        """특정 task만 실행

        Args:
            task_name: 실행할 task 이름
            data: 입력 데이터. dict 또는 list[dict] 가능
                  - None: run.json에서 로드
                  - dict: 단일 입력
                  - list[dict]: 여러 입력 (task 파라미터에 따라 병합 또는 개별 전달)
        """
        # 모든 실행 가능한 task 목록 생성
        all_tasks = list(self.depend)
        if self.on_failure:
            all_tasks.append(self.on_failure)

        # task 검색
        target_task = next((task for task in all_tasks if task.name == task_name), None)

        if not target_task:
            available_tasks = [t.name for t in all_tasks]
            raise ValueError(f'Task "{task_name}"을 찾을 수 없습니다. 사용 가능한 task: {available_tasks}')

        # 입력 데이터 준비
        if data is None:
            input_data = self._load_task_input_from_log(task_name)
            print(f'🚀 Running task: {task_name} (from run.json)')
        else:
            input_data = data
            print(f'🚀 Running task: {task_name}')

        # list[dict]인 경우 처리
        if isinstance(input_data, list):
            sig = inspect.signature(target_task.func)
            param_count = len([p for p in sig.parameters.values() if p.default == inspect.Parameter.empty])

            if len(input_data) == 1:
                # 단일 입력
                input_data = self._inject_env_to_data(input_data[0])
                result = target_task(input_data)
            elif param_count > 1:
                # 여러 파라미터를 받는 task - 개별 전달
                input_data = [self._inject_env_to_data(d) for d in input_data]
                result = target_task(*input_data)
            else:
                # 단일 파라미터 - 병합
                merged = {}
                for d in input_data:
                    merged.update(d)
                merged = self._inject_env_to_data(merged)
                result = target_task(merged)
        else:
            # dict인 경우
            input_data = self._inject_env_to_data(input_data)
            result = target_task(input_data)

        # 결과 출력
        print(f'📤 Output: {json.dumps(result, ensure_ascii=False, indent=2)}')

        return result

    def _load_task_input_from_log(self, task_name):
        """run.json에서 task input 로드

        - task가 이미 실행된 적이 있으면: 해당 task의 input 반환
        - task가 실행된 적이 없으면: 의존하는 task들의 output을 병합하여 반환
        """
        if not os.path.exists(BaseDecorator.run_log_file):
            raise FileNotFoundError(f'{BaseDecorator.run_log_file} 파일이 없습니다. --input-data 인자를 제공하거나 먼저 전체 workflow를 실행하세요.')

        run_log = self._load_log_file()

        # 해당 task가 이미 실행된 적이 있으면 그 input 사용
        if task_name in run_log:
            return run_log[task_name]['input']

        # 실행된 적이 없으면 의존 task들의 output을 병합
        target_task = next((task for task in self.depend if task.name == task_name), None)
        if not target_task or not target_task.depend:
            raise ValueError(f'Task "{task_name}"의 실행 로그가 없고 의존 task도 없습니다. --input-data를 제공하거나 먼저 필요한 task들을 실행하세요.')

        # 의존 task들의 output 수집
        merged_input = {}
        missing_deps = []

        for dep_task in target_task.depend:
            if dep_task.name not in run_log:
                missing_deps.append(dep_task.name)
            else:
                # 의존 task의 output을 병합
                dep_output = run_log[dep_task.name]['output']
                if isinstance(dep_output, dict):
                    merged_input.update(dep_output)

        if missing_deps:
            raise ValueError(f'Task "{task_name}"의 의존 task가 실행되지 않았습니다: {missing_deps}. 먼저 해당 task들을 실행하세요.')

        return merged_input

    def export_airflow(self, output_path=None, schedule_interval=None, start_date=None, default_args=None):
        """
        Airflow DAG로 export

        Args:
            output_path: DAG 파일 저장 경로 (기본: dags/{dag_id}.py)
            schedule_interval: DAG 스케줄 (기본: None)
            start_date: DAG 시작 날짜 (기본: 오늘)
            default_args: DAG default_args

        Returns:
            str: 생성된 DAG 파일 경로
        """
        exporter = AirflowExporter(self)
        return exporter.export(
            output_path=output_path,
            schedule_interval=schedule_interval,
            start_date=start_date,
            default_args=default_args
        )