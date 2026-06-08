# Archive Index — 2026-06

| Feature | Match Rate | Iterations | Archived | Docs |
|---------|:----------:|:----------:|----------|------|
| secret-env | 100% | 0 | 2026-06-09 | [plan](secret-env/secret-env.plan.md) · [design](secret-env/secret-env.design.md) · [analysis](secret-env/secret-env.analysis.md) · [report](secret-env/secret-env.report.md) |

## secret-env — KubernetesPodOperator Secret → env 주입

`@flow`/`@task`에 `secrets=[Secret(...)]` 옵션 추가. K8s Secret을 Airflow 표준 `Secret('env', ...)`로 렌더링하여 KubernetesPodOperator에 env로 주입 (키단위 + 전체 envFrom, flow+task 병합). PDCA 1사이클로 100% 달성.
