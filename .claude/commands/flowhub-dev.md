# FlowhHub Library Development Agent

You are an expert developer for the **FlowhHub** workflow framework. This is a Python library (v1.2.11) for building, managing, and deploying complex data processing pipelines with Kubernetes and Airflow integration.

## Project Overview

FlowhHub is a decorator-based workflow framework that enables:
- Defining workflows and tasks using Python decorators (`@flow`, `@task`)
- Automatic dependency tracking between tasks
- Multi-environment configuration (local dev, Airflow production)
- Kubernetes resource specification per task
- S3/MinIO and local storage abstraction
- Automatic export to Airflow DAGs with ConfigMap-based code delivery

## Architecture

```
flowhub/
├── flowhub/                 # Main package
│   ├── __init__.py         # Package exports
│   ├── base.py             # BaseDecorator - foundation class
│   ├── flow.py             # FlowDecorator - workflow orchestration
│   ├── task.py             # TaskDecorator - task execution
│   ├── action.py           # Action class for dependency tracking
│   ├── model.py            # K8s models (Toleration, VolumeMount, Volume)
│   ├── storage/            # Storage abstraction layer
│   │   ├── __init__.py     # get_storage() factory
│   │   ├── storage.py      # Abstract Storage base class
│   │   ├── local_storage.py # Local filesystem storage
│   │   └── s3_storage.py   # S3/MinIO storage with boto3
│   ├── service/            # Export services
│   │   └── airflow_exporter.py  # Airflow DAG + ConfigMap generation
│   └── template/           # Pre-built processing templates
│       ├── extract_voice_pkg/   # Audio voice extraction
│       ├── analyze_speaker_pkg/ # Speaker analysis
│       ├── transcript_pkg/      # Audio transcription
│       └── read_pdf_pkg/        # PDF processing
├── dags/                   # Generated Airflow DAG files + ConfigMap YAMLs
├── setup.py                # Package configuration (v1.2.11)
└── workflow.py             # Example audiobook processing workflow
```

## Core Components

### 1. FlowDecorator (flow.py)
Orchestrates the entire workflow execution.

**Key Features:**
- CLI argument parsing (`--env`, `--job`, `--task`, `--input`, `--output`, `--run-log`, `--export`)
- Multi-environment configuration
- Automatic dependency graph building via topological sort
- Failure handling with `on_failure` callback
- Airflow DAG export with ConfigMap

**Important Methods:**
- `__init__()` - Initialize with env config and CLI parsing
- `_parse_args()` - Parse command line arguments
- `select_env(env_name)` - Switch environments
- `init()` - Build task dependency graph
- `run(task_name, data)` - Execute specific task
- `export_airflow()` - Convert to Airflow DAG + ConfigMap

### 2. TaskDecorator (task.py)
Defines and executes individual tasks.

**Key Features:**
- Kubernetes resource specification (CPU, memory, GPU)
- Docker image configuration
- Node selector and tolerations
- Volume mounts (merged with common at export time)

**Execution Modes:**
- Action mode: Dependency collection with Action object
- Build mode: Dependency chain with TaskDecorator objects
- Execution mode: Normal execution with dict data

### 3. Storage System (storage/)
Abstract storage layer with factory pattern.

**Classes:**
- `Storage` (abstract base)
- `LocalStorage` - File system based
- `S3Storage` - AWS S3/MinIO with boto3

**Factory:**
```python
def get_storage(env) -> Storage:
    if 'BUCKET' in env:
        return S3Storage(env)
    elif 'WORKSPACE' in env:
        return LocalStorage(env)
```

### 4. AirflowExporter (service/airflow_exporter.py)
Converts FlowhHub workflows to Airflow DAG Python files + ConfigMap YAML.

**Key Features:**
- KubernetesPodOperator generation
- ConfigMap-based code delivery (avoids ARG_MAX limit)
- XCom sidecar support for data passing
- Task dependency wiring
- Resource limit configuration
- Volume mounts merging (task + common)

**Generated Outputs:**
- `dags/{dag_id}_dag.py` - Airflow DAG file
- `dags/{dag_id}-code.yaml` - Kubernetes ConfigMap with workflow code + packages

**Critical Implementation Details:**
1. **ConfigMap contains**: workflow.py (data) + packages (binaryData as tar.gz)
2. **SETUP_SCRIPT**: Copies files from ConfigMap mount to /app
3. **Volume mounts merging**: Task-level mounts MERGE with common, not replace
   ```python
   volume_mounts=common.get('volume_mounts', []) + [task_mount]
   ```
4. **인라인 최적화**: 한 번만 사용되는 변수는 인라인 처리
   - `default_args` → DAG() 생성자에 직접 포함
   - `CONFIGMAP_NAME`, `CONFIGMAP_MOUNT_PATH` → 값을 직접 사용
   - `configmap_volume`, `configmap_volume_mount` → common dict에 직접 포함
5. **포맷팅 함수들**:
   - `_format_dict(d, base_indent=N)`: dict를 Python 코드로 포맷팅 (들여쓰기 지원)
   - `_format_volumes_list(volumes)`: volumes를 여러 줄 포맷으로 변환

## Workflow Execution Flow

1. **Initialization**: `@flow` decorator creates FlowDecorator, parses CLI
2. **Dependency Collection**: `init()` calls main function with Action to collect task dependencies
3. **Topological Sort**: Tasks sorted by dependency depth
4. **Execution**: Tasks run in order, passing data through dict
5. **Persistence**: `run.json` stores intermediate results

## Environment Configuration

```python
env={
    'default': {
        'WORKSPACE': './tmp',
        'DEBUG': True,
        'project': 'my-workflow',
        'run_id': 'run'
    },
    'airflow': {
        'BUCKET': 'millie-flow',
        'REGION_NAME': 'ap-northeast-2',
        'ENDPOINT': 'https://minio.example.com',
        'ACCESS_KEY_ID': '...',
        'SECRET_ACCESS_KEY': '...',
        'K8S_IMAGE': 'python:3.11-slim',
        'K8S_NAMESPACE': 'airflow',
        'EXTRA_PACKAGES': ['./my_package']  # Additional packages to include
    }
}
```

## CLI Usage

```bash
# Run full workflow
python workflow.py --env default --input data.json

# Run single task
python workflow.py --job /path/to/job --task task_name

# Export to Airflow (generates DAG + ConfigMap)
python workflow.py --env airflow --export airflow
```

## Airflow Deployment

```bash
# 1. Export DAG and ConfigMap
python workflow.py --env airflow --export airflow

# 2. Apply ConfigMap to Kubernetes FIRST
kubectl apply -f dags/{dag_id}-code.yaml -n airflow

# 3. Copy DAG file to Airflow DAGs folder
cp dags/{dag_id}_dag.py /path/to/airflow/dags/

# 4. Trigger DAG in Airflow UI
```

## Development Guidelines

1. **Maintain Backward Compatibility**: The library is used in production. Any changes should be backward compatible.

2. **Type Hints**: Use type hints for all public APIs. Follow existing patterns in model.py.

3. **Decorator Pattern**: Core functionality uses decorators. Understand the `__call__` method flow.

4. **Testing**: Test with both local and S3 storage environments.

5. **Airflow Compatibility**: Ensure exported DAGs work with Airflow 2.x and KubernetesPodOperator.

6. **Error Handling**: Use `on_failure` callback pattern for graceful failure handling.

7. **Documentation**: Document args format in docstrings (see template examples).

8. **ConfigMap Size**: Keep ConfigMap under 1MB Kubernetes limit. For larger packages, use git repo approach.

## Key Files to Reference

- **Core Logic**: `flowhub/flow.py` (FlowDecorator), `flowhub/task.py` (TaskDecorator)
- **Storage**: `flowhub/storage/s3_storage.py` (S3 implementation)
- **Export**: `flowhub/service/airflow_exporter.py` (DAG + ConfigMap generation)
- **Example**: `workflow.py` (real-world audiobook processing workflow)
- **Models**: `flowhub/model.py` (K8s dataclasses)

## Common Tasks

When asked to:
- **Add a feature**: Check flow.py and task.py for decorator logic
- **Fix storage issues**: Check storage/__init__.py and s3_storage.py
- **Modify Airflow export**: Edit service/airflow_exporter.py
- **Add K8s options**: Update model.py and task.py
- **Create templates**: Follow patterns in template/ directory
- **Fix ConfigMap issues**: Check _generate_configmap_yaml() and _build_volume_mounts_code()

## Known Issues & Solutions

### 1. "Argument list too long" Error
**Cause**: Passing large base64 data as shell arguments (ARG_MAX limit)
**Solution**: Use ConfigMap-based code delivery (current implementation)

### 2. Task Volume Mounts Overwriting ConfigMap Mount
**Cause**: Task-level `volume_mounts=[...]` replaces common volume_mounts
**Solution**: Use merge pattern in _build_volume_mounts_code():
```python
volume_mounts=common.get('volume_mounts', []) + [task_mount]
```

### 3. SETUP_SCRIPT Newlines Escaped
**Cause**: Using `\\n` produces literal `\n` text instead of newlines
**Solution**: Use actual newlines in f-string

### 4. Package Not Found in EXTRA_PACKAGES
**Cause**: Relative path doesn't exist from current working directory
**Solution**: Use absolute paths or create symlinks

### 5. 인라인 dict 들여쓰기 오류
**Cause**: `_format_dict`에서 인라인 dict 생성 시 들여쓰기 미적용
**Solution**: `base_indent` 파라미터 사용
```python
default_args_str = self._format_dict(default_args, base_indent=1)  # DAG 내부 들여쓰기
```

### 6. volumes/volume_mounts 가독성 문제
**Cause**: 긴 k8s.V1Volume/V1VolumeMount 객체가 한 줄에 출력됨
**Solution**: `_format_volumes_list()` 사용하여 여러 줄로 포맷팅
```python
existing_volumes_list = self._format_volumes_list(common_config_copy['volumes'])
existing_volumes_list.append(configmap_volume)
common_config_copy['volumes'] = '[\n        ' + ',\n        '.join(existing_volumes_list) + '\n    ]'
```

Always run `python workflow.py --env default` to verify changes work locally before testing with Airflow export.
