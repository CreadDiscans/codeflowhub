# FlowhHub Template Development Agent

You are creating a new processing template for the FlowhHub framework.

## Template Structure

Templates are reusable processing modules in `flowhub/template/`.

```
flowhub/template/
├── __init__.py              # Export all templates
├── new_template_pkg/
│   ├── __init__.py          # Export main function
│   └── main.py              # Implementation
```

## Template Pattern

Follow the existing template pattern (e.g., `extract_voice_pkg`):

```python
# flowhub/template/new_template_pkg/main.py

def new_template(args):
    '''
    Brief description of what this template does.

    Required args:
        args['new_template_input']       # Input data (S3 URL or local path)

    Optional args:
        args['new_template_option']      # Optional parameter, default: value

    Returns:
        args['new_template_output']      # Output data location
    '''
    from flowhub.storage import get_storage

    env = args.get('env', {})
    storage = get_storage(env)

    # Get input
    input_url = args.get('new_template_input')
    if not input_url:
        raise ValueError('new_template_input is required')

    # Download input if S3 URL
    if input_url.startswith('s3://'):
        local_input = storage.download(input_url, '/tmp/input')
    else:
        local_input = input_url

    # Process...
    output_path = '/tmp/output'
    # ... processing logic ...

    # Upload output
    output_url = storage.upload(output_path)

    args['new_template_output'] = output_url
    return args
```

## Export the Template

```python
# flowhub/template/new_template_pkg/__init__.py
from .main import new_template

# flowhub/template/__init__.py
from .new_template_pkg import new_template
```

## Using in Workflow

```python
from flowhub import flow, task
from flowhub.template import new_template

@task
def process_data(args):
    return new_template(args)
```

## Template Guidelines

1. **Naming Convention**:
   - Package: `{name}_pkg/`
   - Function: `{name}(args)`
   - Args: `{name}_input`, `{name}_output`, `{name}_*`

2. **Input/Output**:
   - Accept S3 URLs or local paths
   - Use `get_storage(env)` for file operations
   - Return modified args dict

3. **Error Handling**:
   - Validate required args
   - Raise descriptive ValueError for missing inputs

4. **Documentation**:
   - Docstring with all args (Required/Optional)
   - Default values for optional args

5. **Dependencies**:
   - Import heavy dependencies inside function (lazy loading)
   - Document required packages

Now create the requested template: $ARGUMENTS
