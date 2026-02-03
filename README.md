# FlowhHub

Python 기반의 워크플로우 프레임워크로, Kubernetes와 Airflow 환경에서 데이터 처리 파이프라인을 구축할 수 있습니다.

## 설치

```bash
pip install flowhub
```

## 빠른 시작

### 1. 기본 워크플로우 작성

```python
from flowhub import flow, task, get_storage

# Task 정의
@task(image='python:3.11-slim', cpu='1', memory='1Gi')
def step1(args):
    print("Step 1 실행")
    return {**args, 'step1_result': 'done'}

@task(image='python:3.11-slim', cpu='2', memory='2Gi')
def step2(args):
    print("Step 2 실행:", args['step1_result'])
    return {**args, 'step2_result': 'done'}

# Flow 정의
@flow(
    name='my-workflow',
    description='샘플 워크플로우',
    namespace='airflow',
    env={
        'default': {
            'WORKSPACE': './tmp',
            'DEBUG': True
        },
        'airflow': {
            'BUCKET': 'my-bucket',
            'REGION_NAME': 'ap-northeast-2'
        }
    }
)
def main(args):
    _step1 = step1(args)
    _step2 = step2(_step1)
    return _step2
```

### 2. 로컬 실행

```bash
# 전체 워크플로우 실행
python workflow.py --env default --input data.json

# 특정 task만 실행
python workflow.py --job ./tmp/my-job --task step1
```

### 3. Airflow로 내보내기

```bash
# DAG 파일 + ConfigMap YAML 생성
python workflow.py --env airflow --export airflow
```

## 핵심 개념

### @task 데코레이터

개별 작업 단위를 정의합니다.

```python
from flowhub import task, Toleration, VolumeMount

@task(
    image='my-registry/my-image:v1.0',
    cpu='2',
    memory='4Gi',
    gpu='1',  # GPU 사용 시
    tolerations=[Toleration(key='nvidia.com/gpu', operator='Exists', effect='NoSchedule')],
    node_selector={'karpenter.sh/nodepool': 'gpu-nodepool'},
    volume_mounts=[VolumeMount(name='cache', mount_path='/root/.cache')]
)
def my_task(args):
    # args['data']: 입력 데이터
    # args['env']: 환경 변수
    result = do_something(args['data'])
    return {**args, 'my_output': result}
```

**주요 파라미터:**
| 파라미터 | 설명 | 예시 |
|----------|------|------|
| `image` | Docker 이미지 | `'python:3.11'` |
| `cpu` | CPU 요청량 | `'2'`, `'500m'` |
| `memory` | 메모리 요청량 | `'4Gi'`, `'512Mi'` |
| `gpu` | GPU 수 | `'1'` |
| `tolerations` | K8s tolerations | `[Toleration(...)]` |
| `node_selector` | 노드 선택 조건 | `{'key': 'value'}` |
| `volume_mounts` | 볼륨 마운트 | `[VolumeMount(...)]` |

### @flow 데코레이터

워크플로우 전체를 정의합니다.

```python
from flowhub import flow, Volume

@flow(
    name='my-workflow',
    description='워크플로우 설명',
    namespace='airflow',
    env={
        'default': {...},  # 로컬 실행 환경
        'airflow': {...}   # Airflow 환경
    },
    params={'data': {}},  # DAG 파라미터
    tags=['tag1', 'tag2'],
    volumes=[Volume(name='cache', persistent_volume_claim='my-pvc')],
    annotations={'key': 'value'},
    service_account_name='my-sa',
    on_failure=fail_handler,  # 실패 시 호출할 task
    airflow_sidecar_image='my-sidecar:latest'
)
def main(args):
    ...
```

### 환경 설정

```python
env={
    # 로컬 개발 환경
    'default': {
        'WORKSPACE': './tmp',  # 로컬 저장 경로
        'DEBUG': True
    },
    # Airflow/Kubernetes 환경
    'airflow': {
        'BUCKET': 'my-s3-bucket',
        'REGION_NAME': 'ap-northeast-2',
        'ENDPOINT': 'https://minio.example.com',  # MinIO 사용 시
        'ACCESS_KEY': '...',
        'SECRET_KEY': '...',
        'EXTRA_PACKAGES': ['./my_package']  # 추가 패키지 포함
    }
}
```

### Task 의존성

함수 호출 방식으로 의존성을 정의합니다:

```python
@flow(...)
def main(args):
    # 순차 실행
    _a = task_a(args)
    _b = task_b(_a)
    _c = task_c(_b)

    # 병렬 실행 (같은 입력에서 분기)
    _d = task_d(_a)
    _e = task_e(_a)

    # 병합 (여러 task 결과 합치기)
    _f = task_f(_d, _e)

    return _f
```

**실행 순서:**
```
args → task_a → task_b → task_c
            ↘ task_d ↘
                      → task_f
            ↘ task_e ↗
```

### Storage 사용

```python
from flowhub import get_storage

@task(...)
def my_task(args):
    storage = get_storage(args['env'])

    # 다운로드
    local_path = storage.download(args['input_url'], '/tmp')

    # 처리
    output_path = process(local_path)

    # 업로드
    s3_url = storage.upload(output_path)

    return {**args, 'output_url': s3_url}
```

## CLI 명령어

```bash
# 로컬 전체 실행
python workflow.py --env default --input input.json

# 특정 task 실행
python workflow.py --job /path/to/job --task task_name

# Airflow DAG 내보내기
python workflow.py --env airflow --export airflow

# 실행 ID 지정
python workflow.py --env default --id my-run-001 --input input.json
```

### 커스텀 CLI 인자 추가

FlowhHub 파서에 직접 커스텀 인자를 추가할 수 있습니다:

```python
from flowhub import get_parser, parse_args, flow, task

# 1. 파서 가져와서 커스텀 인자 추가
parser = get_parser()
parser.add_argument('--my-option', type=str, default='default')
parser.add_argument('--count', type=int, default=1)
args = parse_args(parser)

# 2. 커스텀 값을 env에 포함하여 flow 정의
@task(image='python:3.11')
def my_task(flow_args):
    print(f"count: {flow_args['env']['COUNT']}")
    return flow_args

@flow(name='my-workflow', env={
    'default': {
        'WORKSPACE': './tmp',
        'COUNT': args.count,           # 커스텀 인자 값 사용
        'MY_OPTION': args.my_option
    }
})
def main(flow_args):
    return my_task(flow_args)
```

실행:
```bash
# flowhub 인자와 커스텀 인자를 함께 사용
python workflow.py --env default --my-option hello --count 5
```

`--help`로 모든 인자 확인:
```bash
python workflow.py --help
```

## Airflow 배포

### 1. DAG + ConfigMap 생성

```bash
python workflow.py --env airflow --export airflow
```

생성되는 파일:
- `dags/{dag_id}_dag.py` - Airflow DAG 파일
- `dags/{dag_id}-code.yaml` - Kubernetes ConfigMap

### 2. 배포 순서

```bash
# 1. ConfigMap 먼저 배포
kubectl apply -f dags/my-workflow-code.yaml -n airflow

# 2. DAG 파일 복사
cp dags/my-workflow_dag.py /path/to/airflow/dags/

# 3. Airflow UI에서 DAG 트리거
```

## 내장 템플릿

FlowhHub는 자주 사용되는 처리 템플릿을 제공합니다:

```python
from flowhub import template

@task(image='my-registry/extract-voice:v1')
def extract_voice(args):
    return template.extract_voice(args)

@task(image='my-registry/transcript:v1')
def transcript(args):
    return template.transcript(args)

@task(image='my-registry/analyze-speaker:v1')
def analyze_speaker(args):
    return template.analyze_speaker(args)

@task(image='my-registry/read-pdf:v1')
def read_pdf(args):
    return template.read_pdf(args)
```

## 실패 처리

```python
@task(image='python:3.11')
def fail_handler(args):
    # args['traceback']: 오류 스택 트레이스
    # args['failed_task']: 실패한 task 이름
    print(f"Task {args['failed_task']} 실패")
    print(args['traceback'])
    # 알림 전송 등 처리
    return args

@flow(
    name='my-workflow',
    on_failure=fail_handler,  # 실패 시 호출
    ...
)
def main(args):
    ...
```

## 프로젝트 구조 예시

```
my-workflow/
├── workflow.py          # 워크플로우 정의
├── my_package/          # 비즈니스 로직 패키지
│   ├── __init__.py
│   └── processor.py
├── dags/                # 생성된 Airflow DAG 파일들
│   ├── my-workflow_dag.py
│   └── my-workflow-code.yaml
└── tmp/                 # 로컬 실행 시 작업 디렉토리
```

## 주의사항

1. **ConfigMap 크기 제한**: Kubernetes ConfigMap은 1MB 제한이 있습니다. 큰 패키지는 git repo 방식을 사용하세요.

2. **환경별 Storage**: `default` 환경은 로컬 파일시스템, `airflow` 환경은 S3/MinIO를 사용합니다.

3. **Task 간 데이터 전달**: `args` dict를 통해 데이터를 전달합니다. 큰 데이터는 Storage에 저장하고 URL만 전달하세요.

4. **Volume Mounts**: GPU 캐시 등 영구 저장이 필요한 경우 PVC와 VolumeMount를 사용하세요.

## 라이선스

Internal Use Only
