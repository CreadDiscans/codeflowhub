# FlowhHub - Workflow Framework for Data Processing Pipelines

## Quick Reference

**Version**: 1.2.11
**Python**: >= 3.8
**Main Use**: Building Kubernetes/Airflow-based data processing workflows

## Project Structure

```
flowhub/
├── flowhub/                 # Main package
│   ├── flow.py             # FlowDecorator - workflow orchestration (CORE)
│   ├── task.py             # TaskDecorator - task execution (CORE)
│   ├── base.py             # BaseDecorator - foundation class
│   ├── action.py           # Action class for dependency tracking
│   ├── model.py            # K8s dataclasses (Toleration, VolumeMount, Volume)
│   ├── storage/            # Storage abstraction (S3, Local)
│   └── service/            # Airflow export service
│       └── airflow_exporter.py  # DAG + ConfigMap generation
├── workflow.py             # Example: audiobook processing workflow
└── dags/                   # Generated Airflow DAGs + ConfigMap YAMLs
```

## Core Concepts

### Decorators
- `@flow` - Define a workflow with environment config
- `@task` - Define a task with K8s resources

### Execution Modes
- **Local**: `--env default` with WORKSPACE
- **Airflow**: `--env airflow` with S3/BUCKET
- **Export**: `--export airflow` generates DAG file + ConfigMap YAML

### Data Flow
- Tasks receive `args` dict, return modified `args`
- `args['env']` contains environment variables
- `run.json` persists intermediate results

## Common Commands

```bash
# Run workflow locally
python workflow.py --env default --input data.json

# Run single task
python workflow.py --job /path/to/job --task task_name

# Export to Airflow (generates DAG + ConfigMap)
python workflow.py --env airflow --export airflow

# Validate generated DAG
python -m py_compile dags/workflow_dag.py

# Deploy ConfigMap first, then DAG
kubectl apply -f dags/workflow-code.yaml -n airflow
```

## Airflow Export Architecture

```
workflow.py --export airflow
    │
    ├── dags/{dag_id}_dag.py      # Airflow DAG with KubernetesPodOperator
    │   ├── SETUP_SCRIPT          # Copies code from ConfigMap to /app
    │   ├── dag = DAG(default_args={...})  # 인라인 default_args
    │   ├── common = {..., 'volumes': [...], 'volume_mounts': [...]}  # 직접 포함
    │   └── Tasks with merged volume_mounts
    │
    └── dags/{dag_id}-code.yaml   # Kubernetes ConfigMap
        ├── data:
        │   └── workflow.py       # Workflow source code
        └── binaryData:
            └── package.tar.gz    # Extra packages (base64)
```

### 최적화된 DAG 구조 (불필요한 변수 제거)
```python
# 제거된 중간 변수들:
# - CONFIGMAP_NAME, CONFIGMAP_MOUNT_PATH (값을 직접 인라인)
# - default_args (DAG 생성자에 직접 포함)
# - configmap_volume, configmap_volume_mount (common dict에 직접 포함)

dag = DAG(
    'workflow-name',
    default_args={...},  # 인라인
    ...
)

common = {
    'volumes': [
        k8s.V1Volume(name='cache', ...),
        k8s.V1Volume(name='code-volume', config_map=...)  # 직접 포함
    ],
    'volume_mounts': [
        k8s.V1VolumeMount(name='code-volume', ...)  # 직접 포함
    ]
}
```

## Critical Patterns

### Volume Mounts Merging (prevents ConfigMap mount loss)
```python
# CORRECT - merges with common
volume_mounts=common.get('volume_mounts', []) + [task_mount]

# WRONG - loses ConfigMap mount!
volume_mounts=[task_mount]
```

### SETUP_SCRIPT Newlines (use actual newlines, not escaped)
```python
# CORRECT
setup_script = f"mkdir -p /app\n                cp /flowhub/code/workflow.py /app/"

# WRONG - produces literal \n
setup_script = f"mkdir -p /app\\n                cp /flowhub/code/workflow.py /app/"
```

## Key Files for Development

| Task | Files |
|------|-------|
| Add flow features | `flowhub/flow.py` |
| Add task options | `flowhub/task.py` |
| Fix storage issues | `flowhub/storage/s3_storage.py` |
| Modify Airflow export | `flowhub/service/airflow_exporter.py` |
| Add K8s models | `flowhub/model.py` |
| Create templates | `flowhub/template/` |
| Fix ConfigMap issues | `_generate_configmap_yaml()`, `_build_volume_mounts_code()` |

## Custom Commands

- `/flowhub-dev` - General development context
- `/add-feature <description>` - Implement new feature
- `/fix-bug <description>` - Debug and fix issues
- `/add-template <name>` - Create processing template
- `/review-airflow` - Review Airflow export

## Known Issues & Solutions

### 1. "Argument list too long" Error
**Cause**: Passing large base64 data as shell arguments (ARG_MAX limit)
**Solution**: Use ConfigMap-based code delivery (current implementation)

### 2. Task Volume Mounts Overwriting ConfigMap Mount
**Cause**: Task-level `volume_mounts=[...]` replaces common volume_mounts
**Solution**: Use merge pattern: `common.get('volume_mounts', []) + [...]`

### 3. SETUP_SCRIPT Newlines Escaped
**Cause**: Using `\\n` produces literal `\n` text instead of newlines
**Solution**: Use actual newlines in f-string

### 4. Package Not Found in EXTRA_PACKAGES
**Cause**: Relative path doesn't exist from current working directory
**Solution**: Use absolute paths or create symlinks

### 5. ConfigMap Size Limit
**Cause**: ConfigMap exceeds 1MB Kubernetes limit
**Solution**: Use git repo approach for larger codebases

### 6. 인라인 dict 들여쓰기 오류
**Cause**: `_format_dict`에서 인라인 dict 생성 시 들여쓰기 미적용
**Solution**: `base_indent` 파라미터 사용 - `_format_dict(default_args, base_indent=1)`

### 7. volumes/volume_mounts 가독성 문제
**Cause**: 긴 k8s 객체가 한 줄에 출력됨
**Solution**: `_format_volumes_list()` 사용하여 여러 줄로 포맷팅

## Development Notes

1. Always test locally first (`--env default`)
2. Check Airflow export after changes
3. Maintain backward compatibility
4. Use type hints for public APIs
5. Follow existing patterns in templates
6. Deploy ConfigMap before DAG file
