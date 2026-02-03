# FlowhHub Feature Development Agent

You are implementing a new feature for the FlowhHub workflow framework.

## Before Starting

1. Read the relevant core files:
   - `flowhub/flow.py` - FlowDecorator (workflow orchestration)
   - `flowhub/task.py` - TaskDecorator (task execution)
   - `flowhub/base.py` - BaseDecorator (foundation)

2. Understand the execution flow:
   - Decorators wrap functions and modify their behavior
   - `__call__` method handles the execution
   - Tasks can run in action/build/execution modes

## Feature Implementation Checklist

- [ ] Identify which module(s) need modification
- [ ] Check if feature affects Airflow export (`service/airflow_exporter.py`)
- [ ] Add type hints for new parameters
- [ ] Update `__init__.py` exports if adding new classes
- [ ] Test with local environment (`--env default`)
- [ ] Test Airflow export (`--export airflow`)
- [ ] Ensure backward compatibility

## Key Patterns

### Adding Flow Parameters
```python
# In flow.py FlowDecorator.__init__
def __init__(self, func=None, *, new_param=None, ...):
    self.new_param = new_param
```

### Adding Task Parameters
```python
# In task.py TaskDecorator
def __init__(self, func=None, *, new_option=None, ...):
    self.new_option = new_option
```

### Adding K8s Models
```python
# In model.py
@dataclass
class NewModel:
    field1: str
    field2: Optional[int] = None
```

## Testing

```bash
# Local test
python workflow.py --env default --input test_data.json

# Single task test
python workflow.py --job ./test_job --task task_name

# Export test
python workflow.py --env airflow --export airflow
cat dags/workflow.py
```

Now implement the requested feature: $ARGUMENTS
