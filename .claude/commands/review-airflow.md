# FlowhHub Airflow Export Review Agent

You are reviewing and improving the Airflow DAG export functionality.

## Key File

The main export logic is in `flowhub/service/airflow_exporter.py`.

## Export Process

1. `FlowDecorator.export_airflow()` is called
2. `AirflowExporter` receives flow configuration
3. Generates:
   - DAG Python file with KubernetesPodOperator for each task
   - ConfigMap YAML file (when not using git repo)

## Code Delivery Methods

### 1. ConfigMap-based (Default)
When `repo` is not set, workflow code and packages are delivered via Kubernetes ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {dag_id}-code
  namespace: airflow
data:
  workflow.py: |
    # workflow source code
binaryData:
  package_name.tar.gz: <base64-encoded-tarball>
```

**Advantages:**
- No git dependency in pods
- Avoids "argument list too long" error (ARG_MAX limit)
- Atomic deployment with DAG

**Deployment:**
```bash
kubectl apply -f dags/{dag_id}-code.yaml -n airflow
```

### 2. Git Repository-based
When `repo` is set in flow configuration:
```python
@flow(repo='https://github.com/org/workflow-repo.git')
```

Pods clone the repository at runtime.

## Generated DAG Structure

최적화된 구조 - 불필요한 중간 변수 제거, 인라인 처리:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Common setup script (ConfigMap에서 코드 복사)
SETUP_SCRIPT = """mkdir -p /app/input
                cp /flowhub/code/workflow.py /app/workflow.py
                tar -xzf /flowhub/code/package.tar.gz -C /app"""

# DAG 정의 - default_args 인라인
dag = DAG(
    'workflow-name',
    default_args={
        'owner': 'flowhub',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Workflow description',
    schedule_interval=None,
    catchup=False,
    tags=['workflow'],
)

# common dict에 volumes/volume_mounts 직접 포함 (중간 변수 없음)
common = {
    'namespace': 'airflow',
    'cmds': ['/bin/sh', '-c'],
    'do_xcom_push': True,
    'volumes': [
        k8s.V1Volume(
            name='cache',
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name='workflow-cache'
            )
        ),
        k8s.V1Volume(
            name='code-volume',
            config_map=k8s.V1ConfigMapVolumeSource(name='workflow-code')
        )
    ],
    'volume_mounts': [
        k8s.V1VolumeMount(
            name='code-volume',
            mount_path='/flowhub/code',
            read_only=True
        )
    ]
}

with dag:
    task1 = KubernetesPodOperator(
        **common,
        task_id='task1',
        image='...',
        # Task-level volume_mounts MERGE with common (not replace!)
        volume_mounts=common.get('volume_mounts', []) + [task_specific_mount],
        ...
    )
```

## Key Components

### ConfigMap Volume Mount
- ConfigMap mounted at `/flowhub/code`
- Contains workflow.py (text) and packages (base64 binaryData)
- SETUP_SCRIPT copies files to /app before execution

### Volume Mounts Merging
**Critical Pattern**: Task-level volume_mounts must MERGE with common, not replace:
```python
# CORRECT - merges ConfigMap mount with task mount
volume_mounts=common.get('volume_mounts', []) + [k8s.V1VolumeMount(...)]

# WRONG - loses ConfigMap mount!
volume_mounts=[k8s.V1VolumeMount(...)]
```

### XCom Sidecar
- Handles data passing between tasks
- Downloads input from S3, uploads output to S3
- Configured via `airflow_sidecar_image` parameter

### Resource Limits
- CPU/Memory requests and limits from TaskDecorator
- GPU support via `limit_gpu`
- Node selector and tolerations

### Failure Handling
- `on_failure` task uses `trigger_rule='one_failed'`
- Receives error information in args

## Review Checklist

- [ ] DAG imports are correct (`from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator`)
- [ ] ConfigMap YAML has valid structure (apiVersion, kind, metadata)
- [ ] ConfigMap namespace matches DAG namespace
- [ ] SETUP_SCRIPT has proper newlines (not escaped `\n`)
- [ ] Task-level volume_mounts MERGE with common (not replace)
- [ ] Task IDs match original task names
- [ ] Dependencies are correctly wired
- [ ] Resource limits are properly formatted
- [ ] EXTRA_PACKAGES paths exist and are included
- [ ] 불필요한 중간 변수 없음 (CONFIGMAP_NAME, CONFIGMAP_MOUNT_PATH, default_args, configmap_volume 등)
- [ ] volumes/volume_mounts가 common dict에 직접 포함됨
- [ ] 들여쓰기가 올바름 (default_args 인라인 시 base_indent 적용)

## Testing Export

```bash
# Generate DAG and ConfigMap
python workflow.py --env airflow --export airflow

# Check outputs
ls -la dags/

# Validate Python syntax
python -m py_compile dags/workflow_dag.py

# Validate ConfigMap YAML structure
python -c "import yaml; yaml.safe_load(open('dags/workflow-code.yaml'))"

# Check ConfigMap size (must be < 1MB)
ls -lh dags/workflow-code.yaml

# Deploy to Kubernetes
kubectl apply -f dags/workflow-code.yaml -n airflow
```

## Common Issues

### 1. "Argument list too long" Error
**Cause**: Passing large base64 data as shell arguments
**Solution**: Use ConfigMap-based approach (current implementation)

### 2. ConfigMap Mount Lost
**Cause**: Task-level `volume_mounts=[...]` replaces common volume_mounts
**Solution**: Use merge pattern: `common.get('volume_mounts', []) + [...]`

### 3. SETUP_SCRIPT Newlines
**Cause**: Using `\\n` produces literal `\n` text
**Solution**: Use actual newlines in f-string

### 4. Package Not Found
**Cause**: EXTRA_PACKAGES path doesn't exist
**Solution**: Use absolute paths or symlinks

### 5. ConfigMap Size Limit
**Cause**: ConfigMap exceeds 1MB Kubernetes limit
**Solution**: Use git repo approach or split packages

### 6. Missing K8s imports
Check `kubernetes.client.models` imports

### 7. Invalid resource format
CPU/Memory must be strings like "100m", "512Mi"

### 8. 들여쓰기 오류 (Indentation Error)
**Cause**: `_format_dict`에서 인라인 dict 생성 시 들여쓰기 미적용
**Solution**: `base_indent` 파라미터 사용 - `_format_dict(default_args, base_indent=1)`

### 9. 긴 줄 가독성 문제
**Cause**: volumes/volume_mounts가 한 줄에 모두 출력됨
**Solution**: `_format_volumes_list()` 사용하여 여러 줄로 포맷팅

## Deployment Order

1. Apply ConfigMap first:
   ```bash
   kubectl apply -f dags/{dag_id}-code.yaml -n airflow
   ```

2. Copy DAG file to Airflow DAGs folder

3. Trigger DAG in Airflow UI

Now review/improve the Airflow export: $ARGUMENTS
