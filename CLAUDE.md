# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Package**: `codeflowhub` (PyPI) — imported as `from codeflowhub import flow, task, ...`
**Version**: 0.0.8
**Python**: >= 3.8
**Purpose**: Workflow framework for building Kubernetes/Airflow-based data processing pipelines

## Package Structure

```
codeflowhub/
├── flow.py              # FlowDecorator — workflow orchestration (CORE)
├── task.py              # TaskDecorator — task execution (CORE)
├── base.py              # BaseDecorator — foundation (run log, dependency tracking)
├── action.py            # Action class — build vs execution mode signaling
├── model.py             # K8s dataclasses: Toleration, VolumeMount, Volume
├── storage/             # Storage abstraction (local + S3/MinIO)
├── service/
│   └── airflow_exporter.py  # DAG + ConfigMap generation
└── template/            # Built-in processing templates (extract_voice, transcript, etc.)
```

## Common Commands

```bash
# Run workflow locally
python workflow.py --env default --input data.json

# Run a single task (resume from job dir)
python workflow.py --job ./tmp/my-job --task task_name

# Export to Airflow (generates DAG + ConfigMap)
python workflow.py --env airflow --export airflow

# Validate generated DAG syntax
python -m py_compile dags/workflow_dag.py

# Build and publish package
bash deploy.sh
```

## Core Architecture

### Decorator Modes

`@task` and `@flow` decorators operate in three modes based on argument type:
- **Build mode**: receives another `TaskDecorator` — sets up dependency graph
- **Action mode**: receives an `Action` object — used internally by flow to trace dependencies
- **Execution mode**: receives a plain dict `args` — runs the actual function

### Data Flow

Tasks receive and return an `args` dict. `args['env']` holds all environment variables. `run.json` persists intermediate results per task in the job directory.

### Dependency Graph

```python
@flow(name='my-workflow', env={'default': {...}, 'airflow': {...}})
def main(args):
    _a = task_a(args)          # sequential
    _b = task_b(_a)
    _c = task_c(_a)            # parallel branch from _a
    _d = task_d(_b, _c)        # merge — waits for both
    return _d
```

### Execution Modes

- **Local** (`--env default`): uses `WORKSPACE` for local file storage
- **Airflow** (`--env airflow`): uses S3/MinIO via `BUCKET` + `REGION_NAME`
- **Export** (`--export airflow`): generates `dags/{dag_id}_dag.py` + `dags/{dag_id}-code.yaml`

## Airflow Export

The exporter in `airflow_exporter.py` generates:

1. **DAG file** (`dags/{dag_id}_dag.py`) — KubernetesPodOperator tasks with a SETUP_SCRIPT that copies code from ConfigMap to `/app`
2. **ConfigMap YAML** (`dags/{dag_id}-code.yaml`) — contains `workflow.py` source and optional extra packages as `binaryData`

Deploy order: ConfigMap first, then DAG file.

### Critical Patterns in Generated DAGs

**Volume mounts must be merged** (not replaced):
```python
# CORRECT — preserves ConfigMap mount
volume_mounts=common.get('volume_mounts', []) + [task_mount]

# WRONG — loses ConfigMap mount
volume_mounts=[task_mount]
```

**SETUP_SCRIPT newlines** — use actual newlines in f-strings, not `\\n`.

## Key Files by Task

| Task | File |
|------|------|
| Add flow parameters | `codeflowhub/flow.py` |
| Add task options | `codeflowhub/task.py` |
| Fix storage issues | `codeflowhub/storage/s3_storage.py` |
| Modify Airflow export | `codeflowhub/service/airflow_exporter.py` |
| Add K8s models | `codeflowhub/model.py` |
| Add processing templates | `codeflowhub/template/` |

## Custom CLI Arguments

```python
from codeflowhub import get_parser, parse_args, flow, task

parser = get_parser()
parser.add_argument('--my-option', type=str)
args = parse_args(parser)

@flow(name='my-workflow', env={'default': {'MY_OPTION': args.my_option, ...}})
def main(flow_args): ...
```

## Custom Commands

- `/add-feature <description>` — Implement new feature
- `/fix-bug <description>` — Debug and fix issues
- `/add-template <name>` — Create processing template
- `/review-airflow` — Review Airflow export output

## Known Pitfalls

1. **Volume mounts overwrite** — task-level `volume_mounts=[...]` replaces common mounts; always merge with `common.get('volume_mounts', []) + [...]`
2. **ConfigMap 1MB limit** — use git repo approach (`repo=` param on `@flow`) for large codebases
3. **EXTRA_PACKAGES path** — relative paths fail from non-cwd; use absolute paths
4. **`_format_dict` indentation** — use `base_indent` param when formatting inline dicts
