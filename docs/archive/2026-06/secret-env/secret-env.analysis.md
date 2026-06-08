# Gap Analysis: secret-env

- **Feature**: `secret-env`
- **Analyzed**: 2026-06-09
- **Phase**: Check
- **Design**: [secret-env.design.md](../02-design/features/secret-env.design.md)

## Context Anchor

| 항목 | 내용 |
|------|------|
| **WHY** | 파이프라인 task가 DB/API 자격증명을 안전하게 수신 (표준 경로 없음) |
| **WHO** | codeflowhub로 Airflow DAG를 export하는 데이터 엔지니어 |
| **RISK** | `**common` + `secrets` 키워드 충돌, 생성 DAG 문법 오류 |
| **SUCCESS** | flow/task 양쪽 키단위·전체 secret → `py_compile` 통과 + `secrets=` 정확 주입 |
| **SCOPE** | Airflow export 경로 env 타입 secret만 |

## 1. Strategic Alignment

| 검증 | 결과 |
|------|------|
| WHY(자격증명 안전 주입) 해결 | ✅ K8s Secret을 코드/ConfigMap 노출 없이 env로 전달 |
| 핵심 Design 결정(Option C, base_secrets 분리) 준수 | ✅ `volume_mounts` 패턴 그대로 구현 |
| 범위(env 타입, Airflow export) 일치 | ✅ volume/로컬모드는 의도적으로 제외 |

## 2. Plan Success Criteria 검증

| SC | 기준 | 상태 | 증거 |
|----|------|------|------|
| SC-1 | 키단위 → `Secret('env','ENV','s','k')` | ✅ Met | [airflow_exporter.py:530-538](../../codeflowhub/service/airflow_exporter.py#L530-L538), 런타임 T-1 출력 일치 |
| SC-2 | 전체 → `Secret('env', None, 's')` | ✅ Met | 동일 메서드, 런타임 T-2 출력 일치 |
| SC-3 | flow → `base_secrets` 분리·병합 | ✅ Met | [airflow_exporter.py:255-261](../../codeflowhub/service/airflow_exporter.py#L255-L261), [540-554](../../codeflowhub/service/airflow_exporter.py#L540-L554) |
| SC-4 | secret 미지정 시 하위호환 | ✅ Met | T-5: no-secret DAG에 secret 아티팩트 0건 |
| SC-5 | 생성 DAG `py_compile` 통과 | ✅ Met | T-6 PASS (secret/no-secret 양쪽) |
| SC-6 | `from codeflowhub import Secret` | ✅ Met | [__init__.py:4,13](../../codeflowhub/__init__.py#L4), T-7 PASS |

**Success Rate: 6/6 (100%)**

## 3. 구조/기능/계약 일치도

### 3.1 Structural Match — 100%

| Design 요소 | 구현 위치 | 상태 |
|-------------|-----------|------|
| `Secret` dataclass | [model.py:24-35](../../codeflowhub/model.py#L24-L35) | ✅ |
| export | [__init__.py:4,13](../../codeflowhub/__init__.py#L4) | ✅ |
| `@task(secrets=)` | [task.py:17,23,35](../../codeflowhub/task.py#L23) | ✅ |
| `@flow(secrets=)` | [flow.py:31,37,61](../../codeflowhub/flow.py#L61) | ✅ |
| `__init__` 수집 + `_has_secrets` | [airflow_exporter.py:77-87](../../codeflowhub/service/airflow_exporter.py#L77-L87) | ✅ |
| 조건부 import | [airflow_exporter.py:148-151](../../codeflowhub/service/airflow_exporter.py#L148-L151) | ✅ |
| `base_secrets` 정의 | [airflow_exporter.py:255-261](../../codeflowhub/service/airflow_exporter.py#L255-L261) | ✅ |
| `_render_secret` | [airflow_exporter.py:530-538](../../codeflowhub/service/airflow_exporter.py#L530-L538) | ✅ |
| `_build_secrets_code` | [airflow_exporter.py:540-554](../../codeflowhub/service/airflow_exporter.py#L540-L554) | ✅ |
| operator 삽입 | [airflow_exporter.py:333-339](../../codeflowhub/service/airflow_exporter.py#L333-L339) | ✅ |

### 3.2 Functional Depth — 100%

- placeholder/TODO 없음, 모든 분기(flow-only/task-only/both/none) 구현
- `__post_init__` 검증 로직 동작 확인 (T-3)
- Edge case 7종(§6) 모두 코드로 커버: on_failure 포함, repo/ConfigMap 모드 무관

### 3.3 API Contract — 100%

Design §5 목표 출력 ↔ 실제 생성 DAG 3-way 일치 (런타임 확인):
- `base_secrets = [Secret('env', None, 'common-secret')]`
- `secrets=base_secrets + [Secret('env', 'DB_PASS', 'db-secret', 'password'), Secret('env', None, 'api-secret')]`
- flow-only task → `secrets=base_secrets,`

## 4. Runtime Verification

| ID | 결과 |
|----|------|
| T-1 키단위 렌더 | ✅ |
| T-2 전체 렌더 | ✅ |
| T-3 ValueError | ✅ |
| T-4 flow+task 병합 (실제 export) | ✅ |
| T-5 하위호환 | ✅ |
| T-6 py_compile | ✅ |
| T-7 import | ✅ |

**Runtime: 7/7 (100%)**

## 5. Decision Record 준수 검증

| 결정 | 준수 | 비고 |
|------|------|------|
| Option C (volume_mounts 패턴 미러링) | ✅ | `base_secrets` + `_build_secrets_code`로 동일 구조 |
| `**common` 충돌 회피 | ✅ | base_secrets 별도 변수, 키워드 중복 없음 (T-4 검증) |
| key단위 + 전체(envFrom) | ✅ | 둘 다 구현 |

## 6. Match Rate

```
Structural × 0.15 = 100 × 0.15 = 15.0
Functional × 0.25 = 100 × 0.25 = 25.0
Contract   × 0.25 = 100 × 0.25 = 25.0
Runtime    × 0.35 = 100 × 0.35 = 35.0
─────────────────────────────────────
Overall = 100%
```

## 7. Gap List

| 심각도 | 항목 | 비고 |
|--------|------|------|
| — | 없음 (Critical/Important 0건) | 모든 SC 충족, 런타임 검증 통과 |

### Minor 관찰 (비차단, 선택적)

1. **(Minor)** Design §8 Test Plan을 ad-hoc 스크립트로 실행했으나 `tests/` 파일로 영속화하지 않음. 단, 현 프로젝트에 테스트 프레임워크/`tests/` 디렉토리가 부재하여 컨벤션상 강제 사항 아님. 회귀 방지를 원하면 후속 추가 권장.
2. **(Non-issue)** `_render_secret`이 secret/key/env_name을 작은따옴표로 감쌈 — K8s 리소스명·env명은 DNS-1123/POSIX 규칙상 따옴표 불가하므로 인젝션 위험 없음.

## 8. 결론

Match Rate **100%** (≥90% 통과). Critical/Important gap 없음. **Report 단계 진행 권장.**
