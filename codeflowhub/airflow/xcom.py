"""Airflow REST API client for fetching task data at runtime.

Used inside KubernetesPodOperator containers to fetch xcom/params data
via Airflow REST API instead of inlining it in shell arguments (ARG_MAX safe).

Environment variables (injected via Airflow Connection):
  - FLOWHUB_API_URL: Airflow webserver URL (e.g. http://airflow-web:8080)
  - FLOWHUB_API_USER: Basic auth user
  - FLOWHUB_API_PASS: Basic auth password

Usage:
  python3 -m codeflowhub.airflow.xcom xcom <dag_id> <run_id> <task_id> <output_path>
  python3 -m codeflowhub.airflow.xcom params <dag_id> <run_id> <output_path>
"""
import urllib.request
import json
import base64
import os
import sys
from urllib.parse import quote


def _api_request(path: str):
    """Send authenticated GET request to Airflow REST API.

    Args:
        path: API path (e.g. /api/v1/dags/...)

    Returns:
        parsed JSON response body
    """
    required_vars = ['FLOWHUB_API_URL', 'FLOWHUB_API_USER', 'FLOWHUB_API_PASS']
    missing = [v for v in required_vars if v not in os.environ]
    if missing:
        print(f"Error: missing environment variables: {', '.join(missing)}", file=sys.stderr)
        print("These should be injected via Airflow Connection (FLOWHUB_API_URL, FLOWHUB_API_USER, FLOWHUB_API_PASS)", file=sys.stderr)
        sys.exit(1)

    auth = base64.b64encode(
        (os.environ['FLOWHUB_API_USER'] + ':' + os.environ['FLOWHUB_API_PASS']).encode()
    ).decode()

    url = os.environ['FLOWHUB_API_URL'].rstrip('/') + path

    req = urllib.request.Request(
        url,
        headers={
            'Authorization': 'Basic ' + auth,
            'Accept': 'application/json',
        },
    )

    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print(f"Error: authentication failed for {url}", file=sys.stderr)
            print("Check FLOWHUB_API_USER / FLOWHUB_API_PASS in Airflow Connection", file=sys.stderr)
        elif e.code == 404:
            print(f"Error: resource not found: {url}", file=sys.stderr)
        else:
            print(f"Error: HTTP {e.code} from {url}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Error: cannot reach Airflow API at {os.environ['FLOWHUB_API_URL']}: {e.reason}", file=sys.stderr)
        sys.exit(1)

    return json.loads(resp.read())


def _write_json(content, output_path: str):
    """Write JSON content to file."""
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as f:
        if isinstance(content, str):
            f.write(content)
        else:
            json.dump(content, f)


def fetch_xcom(dag_id: str, run_id: str, task_id: str, output_path: str):
    """Fetch xcom value from Airflow REST API and save to file.

    Args:
        dag_id: Airflow DAG ID
        run_id: DAG Run ID
        task_id: upstream task ID
        output_path: path to save the JSON output
    """
    path = (
        '/api/v1/dags/' + quote(dag_id, safe='')
        + '/dagRuns/' + quote(run_id, safe='')
        + '/taskInstances/' + quote(task_id, safe='')
        + '/xcomEntries/return_value'
    )
    body = _api_request(path)
    _write_json(body['value'], output_path)


def fetch_params(dag_id: str, run_id: str, output_path: str):
    """Fetch DAG run params (conf) from Airflow REST API and save to file.

    Args:
        dag_id: Airflow DAG ID
        run_id: DAG Run ID
        output_path: path to save the JSON output
    """
    path = (
        '/api/v1/dags/' + quote(dag_id, safe='')
        + '/dagRuns/' + quote(run_id, safe='')
    )
    body = _api_request(path)
    _write_json(body.get('conf', {}), output_path)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:", file=sys.stderr)
        print("  python3 -m codeflowhub.airflow.xcom xcom <dag_id> <run_id> <task_id> <output_path>", file=sys.stderr)
        print("  python3 -m codeflowhub.airflow.xcom params <dag_id> <run_id> <output_path>", file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]

    if command == 'xcom':
        if len(sys.argv) != 6:
            print("Usage: python3 -m codeflowhub.airflow.xcom xcom <dag_id> <run_id> <task_id> <output_path>", file=sys.stderr)
            sys.exit(1)
        fetch_xcom(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

    elif command == 'params':
        if len(sys.argv) != 5:
            print("Usage: python3 -m codeflowhub.airflow.xcom params <dag_id> <run_id> <output_path>", file=sys.stderr)
            sys.exit(1)
        fetch_params(sys.argv[2], sys.argv[3], sys.argv[4])

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Available commands: xcom, params", file=sys.stderr)
        sys.exit(1)
