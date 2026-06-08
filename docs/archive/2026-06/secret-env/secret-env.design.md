# Design: KubernetesPodOperator Secret → env 주입

- **Feature**: `secret-env`
- **Created**: 2026-06-09
- **Status**: Design
- **Selected Architecture**: Option C — Pragmatic Balance (기존 `_build_*_code` + `base_*` 병합 패턴 미러링)
- **Plan**: [secret-env.plan.md](../../01-plan/features/secret-env.plan.md)

## Context Anchor

| 항목 | 내용 |
|------|------|
| **WHY** | 데이터 파이프라인 task가 DB/API 자격증명을 안전하게 받아야 하는데 표준 경로가 없다. |
| **WHO** | codeflowhub로 Airflow DAG를 export하는 데이터 엔지니어. |
| **RISK** | `**common` 스프레드와 `secrets` 키워드 중복 충돌, 생성 DAG 문법 오류. |
| **SUCCESS** | Flow/Task 양쪽에서 키단위·전체 secret 선언 → 생성 DAG가 `py_compile` 통과 + `secrets=` 정확 주입. |
| **SCOPE** | Airflow export 경로의 env 타입 secret만. volume 타입·로컬 실행 모드 제외. |

## 1. Overview

`@flow`/`@task` 데코레이터에 `secrets=[Secret(...)]` 옵션을 추가하고, `AirflowExporter`가 이를 Airflow 표준 `Secret('env', deploy_target, secret, key)` 호출로 렌더링하여 `KubernetesPodOperator(secrets=[...])`에 주입한다. 구조는 기존 `volume_mounts` 처리 방식([airflow_exporter.py:466-497](../../../codeflowhub/service/airflow_exporter.py#L466-L497))을 그대로 따른다.

## 2. 데이터 모델

### 2.1 `Secret` dataclass ([model.py](../../../codeflowhub/model.py))

```python
@dataclass
class Secret:
    secret: str                       # K8s Secret 리소스 이름 (필수)
    key: Optional[str] = None         # secret 내 특정 key. None이면 전체 키를 env로 (envFrom)
    env_name: Optional[str] = None    # 주입할 환경변수명. key 지정 시 필수

    def __post_init__(self):
        if self.key is not None and not self.env_name:
            raise ValueError(
                "Secret: 'key'를 지정하면 'env_name'도 필요합니다. "
                "전체 secret 주입은 key/env_name 없이 Secret(secret='name')을 사용하세요."
            )
```

**매핑 규칙** → Airflow `Secret(deploy_type, deploy_target, secret, key)`:

| 사용 형태 | 의미 | 렌더링 결과 |
|-----------|------|-------------|
| `Secret(secret='db', key='password', env_name='DB_PASS')` | 키 단위 | `Secret('env', 'DB_PASS', 'db', 'password')` |
| `Secret(secret='db')` | 전체(envFrom) | `Secret('env', None, 'db')` |

### 2.2 Export ([__init__.py](../../../codeflowhub/__init__.py))

```python
from .model import VolumeMount, Toleration, Volume, SidecarContainer, Secret
__all__ = [..., 'Secret']
```

## 3. 데코레이터 변경

### 3.1 TaskDecorator ([task.py:18-32](../../../codeflowhub/task.py#L18-L32))

```python
from .model import Toleration, VolumeMount, SidecarContainer, Secret

class TaskDecorator(BaseDecorator):
    secrets: list[Secret]

    def __init__(self, *args, ..., secrets: list[Secret] = None, **kwargs):
        ...
        self.secrets = secrets or []
```

### 3.2 FlowDecorator ([flow.py:33-59](../../../codeflowhub/flow.py#L33-L59))

```python
def __init__(self, *args, ..., secrets=None, **kwargs):
    ...
    self.secrets = secrets or []
```

## 4. Exporter 렌더링 ([airflow_exporter.py](../../../codeflowhub/service/airflow_exporter.py))

### 4.1 `__init__` — 상태 수집

```python
# Flow-level secrets
self.flow_secrets = getattr(flow_decorator, 'secrets', None) or []

# secret 존재 여부 (import 생성 조건). on_failure 포함
_all_tasks = list(flow_decorator.depend or [])
if flow_decorator.on_failure:
    _all_tasks.append(flow_decorator.on_failure)
self._has_secrets = bool(self.flow_secrets) or any(
    getattr(t, 'secrets', None) for t in _all_tasks
)
```

### 4.2 header — 조건부 import ([airflow_exporter.py:126-134](../../../codeflowhub/service/airflow_exporter.py#L126-L134) 직후)

```python
if self._has_secrets:
    header += '''
from airflow.providers.cncf.kubernetes.secret import Secret
'''
```

### 4.3 `base_secrets` 정의 생성 (`_generate_dag_code`, common_definition 직후)

`base_secrets`는 **flow-level secret이 있을 때만** 생성한다 (없으면 task가 직접 `secrets=[...]` 렌더).

```python
secrets_definition = ""
if self.flow_secrets:
    items = [self._render_secret(s) for s in self.flow_secrets]
    secrets_definition = "# Base secrets (flow-level)\nbase_secrets = [\n    " \
        + ",\n    ".join(items) + "\n]\n\n"
# return: header + dag_definition + common_definition + secrets_definition + tasks_code
```

### 4.4 신규 헬퍼 메서드

```python
def _render_secret(self, s):
    """단일 Secret → Airflow Secret('env', ...) 코드 문자열"""
    if s.key is None:
        return f"Secret('env', None, '{s.secret}')"      # 전체(envFrom)
    return f"Secret('env', '{s.env_name}', '{s.secret}', '{s.key}')"

def _build_secrets_code(self, task):
    """Task secrets 렌더 + flow-level base_secrets 병합

    volume_mounts 패턴과 동일: base_secrets는 **common이 아닌 별도 변수로 병합.
    """
    task_items = [self._render_secret(s) for s in (getattr(task, 'secrets', None) or [])]
    has_base = bool(self.flow_secrets)

    if has_base and task_items:
        return f"\n        secrets=base_secrets + [{', '.join(task_items)}],"
    elif has_base:
        return "\n        secrets=base_secrets,"
    elif task_items:
        return f"\n        secrets=[{', '.join(task_items)}],"
    return ""
```

### 4.5 `_generate_task_operator` 삽입 ([airflow_exporter.py:287-312](../../../codeflowhub/service/airflow_exporter.py#L287-L312))

```python
secrets_code = self._build_secrets_code(task)   # 다른 _build_*_code 옆에 추가

operator_code = f'''    {task.name} = KubernetesPodOperator(
        **common,
        task_id='{task.name}',
        image='{task_image}',{pool_code}{trigger_rule_code}{retries_code}{env_vars_code}{secrets_code}{tolerations_code}{node_selector_code}{affinity_code}{volume_mounts_code}{container_resources_code}{sidecars_code}
        arguments=[...],
    )'''
```

> **함정 주의**: `secrets`를 `common_config`에 넣어 `**common`으로 펼치면 task-level `secrets=`와 키워드 중복 → `TypeError`. 반드시 `base_secrets` 변수로 분리. (Plan RISK / [CLAUDE.md Known Pitfalls #1](../../../CLAUDE.md))

## 5. 생성 코드 예시 (목표 출력)

입력:
```python
@flow(name='wf', env={...}, secrets=[Secret(secret='common-secret')])
def main(args):
    a = step_a(args)
    return a

@task(secrets=[
    Secret(secret='db-secret', key='password', env_name='DB_PASS'),
    Secret(secret='api-secret'),
])
def step_a(args): ...
```

출력 DAG (발췌):
```python
from airflow.providers.cncf.kubernetes.secret import Secret

# Base secrets (flow-level)
base_secrets = [
    Secret('env', None, 'common-secret')
]

with dag:
    step_a = KubernetesPodOperator(
        **common,
        task_id='step_a',
        image='...',
        secrets=base_secrets + [Secret('env', 'DB_PASS', 'db-secret', 'password'), Secret('env', None, 'api-secret')],
        ...
    )
```

## 6. Edge Cases

| 케이스 | 처리 |
|--------|------|
| secret 전혀 없음 | import·base_secrets·`secrets=` 모두 미생성 → 기존 출력과 동일 (하위 호환, NFR-1) |
| flow만 secret | 모든 task `secrets=base_secrets,` |
| task만 secret | 해당 task만 `secrets=[...]`, base_secrets 변수 미생성 |
| flow+task 동시 | `secrets=base_secrets + [...]` |
| `key` 있고 `env_name` 없음 | `Secret.__post_init__`에서 `ValueError` (조기 실패) |
| `on_failure` task의 secret | `_has_secrets` 및 `_build_secrets_code`에서 동일 처리 |
| `repo` 모드 / ConfigMap 모드 | base_secrets는 모드 무관하게 동일 위치 생성 (common 분기 영향 없음) |

## 7. 변경 파일 요약

| 파일 | 변경 |
|------|------|
| [model.py](../../../codeflowhub/model.py) | `Secret` dataclass 추가 |
| [__init__.py](../../../codeflowhub/__init__.py) | `Secret` import/export |
| [task.py](../../../codeflowhub/task.py) | `secrets` 파라미터 |
| [flow.py](../../../codeflowhub/flow.py) | `secrets` 파라미터 |
| [airflow_exporter.py](../../../codeflowhub/service/airflow_exporter.py) | `__init__` 수집, import, `base_secrets`, `_render_secret`, `_build_secrets_code`, operator 삽입 |

## 8. Test Plan

| ID | 레벨 | 시나리오 | 기대 |
|----|------|----------|------|
| T-1 | unit | `Secret(secret='db', key='password', env_name='DB_PASS')` 렌더 | `Secret('env', 'DB_PASS', 'db', 'password')` |
| T-2 | unit | `Secret(secret='db')` 렌더 | `Secret('env', None, 'db')` |
| T-3 | unit | `Secret(secret='db', key='k')` (env_name 누락) | `ValueError` |
| T-4 | integration | flow+task secret 동시 export | DAG에 import + `base_secrets` + `secrets=base_secrets + [...]` |
| T-5 | integration | secret 미지정 export | 기존 출력과 동일 (회귀) |
| T-6 | smoke | 생성 DAG 검증 | `python -m py_compile dags/wf_dag.py` 통과 |
| T-7 | smoke | `from codeflowhub import Secret` | import 성공 |

## 9. Success Criteria 매핑

SC-1→T-1, SC-2→T-2, SC-3→T-4, SC-4→T-5, SC-5→T-6, SC-6→T-7 (Plan §3)

## 11. Implementation Guide

### 11.1 구현 순서

1. `model.py`: `Secret` dataclass + `__post_init__` 검증
2. `__init__.py`: export
3. `task.py`: `secrets` 파라미터
4. `flow.py`: `secrets` 파라미터
5. `airflow_exporter.py`: `__init__` 수집 → import → `base_secrets` → `_render_secret`/`_build_secrets_code` → operator 삽입
6. 검증용 임시 workflow로 export 후 `py_compile`

### 11.2 Module Map

| Module | 파일 | 핵심 |
|--------|------|------|
| module-1 | model.py, __init__.py | `Secret` 모델 + export |
| module-2 | task.py, flow.py | `secrets` 파라미터 (양쪽) |
| module-3 | airflow_exporter.py | 렌더링 5개 변경점 |

### 11.3 Session Guide

- **Session 1 (`--scope module-1,module-2`)**: 모델 + 데코레이터. 의존성 없음, API 표면 먼저 확정.
- **Session 2 (`--scope module-3`)**: exporter 렌더링 (함정 집중 구간). module-1 완료 필요.

> 범위가 작아 **단일 세션 일괄 구현(`/pdca do secret-env`)**도 권장 가능.

## 12. Out of Scope

- volume 타입 secret 마운트
- 로컬 실행 모드(`--env default`) secret 처리
- Secret 리소스 생성/관리 (사용자가 K8s에 사전 생성 가정)
