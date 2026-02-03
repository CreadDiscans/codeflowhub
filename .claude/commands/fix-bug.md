# FlowhHub Bug Fix Agent

You are debugging and fixing a bug in the FlowhHub workflow framework.

## Debugging Strategy

1. **Reproduce the issue** first
2. **Identify the affected module**:
   - Workflow execution issues → `flow.py`
   - Task execution issues → `task.py`
   - Storage/file issues → `storage/` directory
   - Airflow export issues → `service/airflow_exporter.py`

3. **Trace the execution flow**:
   - Check `run.json` for intermediate results
   - Add debug logging if needed
   - Test with `--task` flag to isolate specific tasks

## Common Bug Areas

### CLI Argument Issues (flow.py)
- `_parse_args()` method handles all CLI parsing
- Check argument defaults and type conversions

### Dependency Issues (flow.py)
- `init()` builds dependency graph
- `_level_sorted_tasks()` sorts by dependency depth
- Action/Build mode detection in task.py `__call__`

### Storage Issues (storage/)
- S3 URL format: `s3://{bucket}/{key}`
- HTTPS URL conversion in `s3_storage.py`
- Path handling in `local_storage.py`

### Airflow Export Issues (service/airflow_exporter.py)
- Task operator generation
- XCom sidecar configuration
- Dependency wiring

## Debugging Commands

```bash
# Run with debug output
DEBUG=true python workflow.py --env default --input test.json

# Check run log
cat run.json | python -m json.tool

# Test single task
python workflow.py --job ./job_dir --task specific_task

# Verify Airflow output
python workflow.py --export airflow && cat dags/workflow.py
```

## Key Files

| Issue Type | Primary File | Secondary Files |
|------------|--------------|-----------------|
| Execution | flow.py | task.py, base.py |
| Storage | storage/s3_storage.py | storage/local_storage.py |
| Airflow | service/airflow_exporter.py | flow.py |
| Models | model.py | task.py |

Now investigate and fix the reported issue: $ARGUMENTS
