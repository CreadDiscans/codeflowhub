# Plan: KubernetesPodOperator Secret → env 주입

- **Feature**: `secret-env`
- **Created**: 2026-06-09
- **Status**: Plan
- **Package**: codeflowhub (v0.0.8)

## Executive Summary

| 관점 | 내용 |
|------|------|
| **Problem** | 현재 codeflowhub은 K8s Secret 값을 Pod 환경변수로 주입하는 표준 방법이 없다. `airflow_connection_id` 기반 `env_vars`만 한정적으로 지원한다. |
| **Solution** | `@flow`/`@task`에 `secrets=[Secret(...)]` 옵션을 추가하고, Airflow 표준 `Secret('env', ...)` 클래스로 KubernetesPodOperator에 렌더링한다. |
| **Function/UX Effect** | 사용자가 `Secret(secret='db', key='password', env_name='DB_PASS')`(키 단위) 또는 `Secret(secret='db')`(전체 envFrom)로 선언하면 DAG에 자동 주입된다. |
| **Core Value** | 민감정보를 코드/ConfigMap에 노출하지 않고 K8s Secret으로 안전하게 컨테이너에 전달. 기존 task 옵션 패턴과 일관된 선언적 API. |

## Context Anchor

| 항목 | 내용 |
|------|------|
| **WHY** | 데이터 파이프라인 task가 DB/API 자격증명을 안전하게 받아야 하는데 표준 경로가 없다. |
| **WHO** | codeflowhub로 Airflow DAG를 export하는 데이터 엔지니어. |
| **RISK** | `**common` 스프레드와 `secrets` 키워드 중복 충돌(volume_mounts와 동일 함정), 생성 DAG 문법 오류. |
| **SUCCESS** | Flow/Task 양쪽에서 키단위·전체 secret을 선언 → 생성 DAG가 `py_compile` 통과 + KubernetesPodOperator에 `secrets=` 정확히 주입. |
| **SCOPE** | Airflow export 경로(`airflow_exporter.py`)의 env 타입 secret만. volume 타입 secret, 로컬 실행 모드 secret은 범위 외. |

## 1. 요구사항 (Requirements)

### Functional Requirements

| ID | 요구사항 | 확정 사항 |
|----|----------|-----------|
| FR-1 | `Secret` 모델 정의 | `model.py`에 dataclass 추가, `__init__.py`에서 export |
| FR-2 | Task 레벨 지정 | `@task(secrets=[Secret(...)])` 지원 |
| FR-3 | Flow 레벨(공통) 지정 | `@flow(secrets=[Secret(...)])` → 모든 task 공통 주입 |
| FR-4 | 키 단위 매핑 | `Secret(secret='s', key='k', env_name='ENV')` → `Secret('env','ENV','s','k')` |
| FR-5 | Secret 전체(envFrom) | `Secret(secret='s')` → `Secret('env', None, 's')` (모든 키를 env로) |
| FR-6 | Flow+Task 병합 | task 레벨 secret은 flow 레벨 secret에 **추가**(덮어쓰기 아님) |
| FR-7 | 조건부 import | secret이 하나라도 있을 때만 `from airflow.providers.cncf.kubernetes.secret import Secret` 생성 |

### Non-Functional Requirements

| ID | 요구사항 |
|----|----------|
| NFR-1 | 기존 DAG 생성 결과 하위 호환 (secrets 미지정 시 출력 변화 없음) |
| NFR-2 | 생성된 DAG는 `python -m py_compile` 통과 |
| NFR-3 | 기존 task 옵션 패턴(`volume_mounts` 병합 방식)과 코드 스타일 일관성 유지 |

## 2. 설계 결정 (확정)

1. **적용 범위**: Flow + Task 둘 다 (병합)
2. **주입 단위**: 키 단위 매핑 + Secret 전체(envFrom) 둘 다
3. **구현 방식**: Airflow `Secret` 클래스 (`secrets=[...]` 파라미터)

### 제안 API

```python
from codeflowhub import flow, task, Secret

@flow(name='wf', env={...}, secrets=[
    Secret(secret='common-secret'),                                  # 전체 키 → env (envFrom)
])
def main(args):
    a = step_a(args)
    return a

@task(secrets=[
    Secret(secret='db-secret', key='password', env_name='DB_PASS'),  # 키 단위
    Secret(secret='api-secret'),                                     # 전체
])
def step_a(args):
    ...
```

### 생성 코드 형태 (목표)

```python
from airflow.providers.cncf.kubernetes.secret import Secret

# Base secrets (flow-level)
base_secrets = [
    Secret('env', None, 'common-secret'),
]

with dag:
    step_a = KubernetesPodOperator(
        **common,
        task_id='step_a',
        ...
        secrets=base_secrets + [
            Secret('env', 'DB_PASS', 'db-secret', 'password'),
            Secret('env', None, 'api-secret'),
        ],
        ...
    )
```

> **핵심 함정**: `volume_mounts`처럼 `secrets`도 `**common`에 넣으면 task 레벨과 키워드 중복 충돌. 반드시 `base_secrets` 변수로 분리 후 `base_secrets + [...]` 병합. (참조: [airflow_exporter.py:221](codeflowhub/service/airflow_exporter.py#L221), [Known Pitfalls #1](CLAUDE.md))

## 3. Success Criteria

| SC | 기준 | 검증 방법 |
|----|------|-----------|
| SC-1 | Task 레벨 키단위 secret이 `Secret('env','ENV','s','k')`로 렌더링 | 단위 검증 + 생성물 grep |
| SC-2 | Secret 전체가 `Secret('env', None, 's')`로 렌더링 | 생성물 grep |
| SC-3 | Flow 레벨 secret이 `base_secrets`로 분리되어 모든 task에 `base_secrets + [...]` 병합 | 생성물 검사 |
| SC-4 | secret 미지정 시 기존 출력과 동일 (하위 호환) | 회귀 비교 |
| SC-5 | 생성 DAG가 `python -m py_compile` 통과 | 명령 실행 |
| SC-6 | `from codeflowhub import Secret` 동작 | import 테스트 |

## 4. Risks & Mitigation

| 위험 | 영향 | 완화책 |
|------|------|--------|
| `secrets` 키워드 중복으로 `**common` 충돌 | DAG 런타임 에러 | `base_secrets` 분리 (volume_mounts 패턴 그대로) |
| `repo` 모드 vs ConfigMap 모드에서 `base_secrets` 위치 차이 | 변수 미정의 NameError | secret 유무로만 `base_secrets` 생성, repo 모드와 무관하게 동작하도록 배치 |
| Airflow provider 버전별 `Secret` import 경로 차이 | import 실패 | 표준 경로 `airflow.providers.cncf.kubernetes.secret` 사용(현 provider 표준) |

## 5. Implementation Guide

### Module Map

| Module | 파일 | 작업 |
|--------|------|------|
| module-1: Model | `codeflowhub/model.py`, `codeflowhub/__init__.py` | `Secret` dataclass 추가 + export |
| module-2: Decorators | `codeflowhub/task.py`, `codeflowhub/flow.py` | `secrets` 파라미터 추가 |
| module-3: Exporter | `codeflowhub/service/airflow_exporter.py` | import 생성, `base_secrets` 생성, `_build_secrets_code()` |

### 11.3 Session Guide (권장 세션 분할)

- **Session 1 (`--scope module-1,module-2`)**: 모델 + 데코레이터 (API 표면). 의존성 없음, 먼저 진행.
- **Session 2 (`--scope module-3`)**: exporter 렌더링 로직 (가장 복잡, 함정 집중 구간).

### 구현 순서 체크리스트

1. `model.py`: `Secret` dataclass (`secret`, `key=None`, `env_name=None`)
2. `__init__.py`: `Secret` import/`__all__`/export
3. `task.py`: `TaskDecorator.__init__`에 `secrets=None` → `self.secrets = secrets or []`
4. `flow.py`: `FlowDecorator.__init__`에 `secrets=None` → `self.secrets`
5. `airflow_exporter.py`:
   - `__init__`: `self.flow_secrets = flow_decorator.secrets`
   - header: secret 존재 시 `Secret` import 추가
   - `base_secrets` 변수 생성 (flow secret)
   - `_build_secrets_code(task)`: task secret 렌더 + `base_secrets` 병합 → `_generate_task_operator`에 삽입
6. `python -m py_compile`로 생성 DAG 검증

## 6. Out of Scope

- volume 타입 secret 마운트 (`deploy_type='volume'`)
- 로컬 실행 모드(`--env default`)에서의 secret 처리
- Secret 리소스 자체의 생성/관리 (사용자가 K8s에 미리 생성한다고 가정)
