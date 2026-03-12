import os
import base64
import sys
import hashlib
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

class AirflowExporter:
    """Airflow DAG로 export하는 서비스"""

    def __init__(self, flow_decorator):
        self.flow = flow_decorator
        self.dag_id = (flow_decorator.flow_name or flow_decorator.name).replace('_', '-')

        # workflow 파일명 추출
        workflow_file_path = sys.modules[flow_decorator.func.__module__].__file__
        self.workflow_filename = os.path.basename(workflow_file_path)

        # 선택된 환경의 설정 사용
        self.env = flow_decorator.env
        self.current_env = flow_decorator.current_env

        # K8s 설정
        self.k8s_image = self.env.get('K8S_IMAGE', 'python:3.11-slim')
        self.k8s_namespace = self.env.get('K8S_NAMESPACE', 'airflow')

        # XCom sidecar image: flow의 airflow_sidecar_image를 우선 사용, 없으면 env에서
        self.xcom_sidecar_image = flow_decorator.airflow_sidecar_image or self.env.get('XCOM_SIDECAR_IMAGE', None)

        # 추가 패키지 경로들 (env에서 설정 가능)
        self.extra_packages = self.env.get('EXTRA_PACKAGES', [])

        # ConfigMap 설정
        self.configmap_name = f'{self.dag_id}-code'
        self.configmap_mount_path = '/flowhub/code'

        # Common K8s 설정
        self.common_config = self.env.get('K8S_COMMON', {
            'namespace': self.k8s_namespace,
            'cmds': ['/bin/sh', '-c'],
            'do_xcom_push': True,
            'startup_timeout_seconds': 3600,
        })

        # 커스텀 CLI 인자 가져오기 (FlowDecorator에서 저장된 값)
        from ..flow import FlowDecorator
        self.custom_cli_args = FlowDecorator._custom_cli_args.copy()

        # Flow의 annotations, service_account_name, volumes가 있으면 common_config에 추가
        if flow_decorator.annotations:
            self.common_config['annotations'] = flow_decorator.annotations
        if flow_decorator.service_account_name:
            self.common_config['service_account_name'] = flow_decorator.service_account_name
        # emptyDir 볼륨은 common이 아닌 full_pod_spec에 task별로 주입됨
        self.empty_dir_volumes = {}

        if flow_decorator.volumes:
            # Volume 객체를 k8s.V1Volume 형태로 변환
            volumes_list = []
            for vol in flow_decorator.volumes:
                if not hasattr(vol, 'name'):
                    continue
                if hasattr(vol, 'persistent_volume_claim') and vol.persistent_volume_claim:
                    volumes_list.append({
                        'name': vol.name,
                        'persistent_volume_claim': vol.persistent_volume_claim
                    })
                elif hasattr(vol, 'empty_dir') and vol.empty_dir is not None:
                    self.empty_dir_volumes[vol.name] = vol
            if volumes_list:
                self.common_config['volumes'] = volumes_list

    def export(self,
               output_path: str = None,
               schedule_interval: Optional[str] = None,
               start_date: Optional[datetime] = None,
               default_args: Optional[dict] = None):
        """Airflow DAG Python 파일로 export"""
        if output_path is None:
            output_path = f'dags/{self.dag_id}_dag.py'

        if start_date is None:
            start_date = datetime(2024, 1, 1)

        if default_args is None:
            default_args = {
                'owner': 'flowhub',
                'start_date': start_date,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            }

        # 디렉토리 생성
        dir_path = os.path.dirname(output_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        # ConfigMap YAML 생성 (repo가 없을 때만)
        configmap_path = None
        if not self.flow.repo:
            configmap_path = self._generate_configmap_yaml(dir_path)

        # DAG 코드 생성
        dag_code = self._generate_dag_code(schedule_interval, start_date, default_args)

        # 파일 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(dag_code)
            f.flush()
            os.fsync(f.fileno())

        print(f'✅ Airflow DAG exported to: {output_path}')
        if configmap_path:
            print(f'✅ ConfigMap YAML exported to: {configmap_path}')
            print(f'📋 Apply ConfigMap: kubectl apply -f {configmap_path}')
        return output_path

    def _generate_dag_code(self, schedule_interval, start_date, default_args):
        """DAG Python 코드 생성 (target.py 스타일)"""

        flow_name = self.flow.flow_name or self.flow.name
        header = f'''"""
Airflow DAG for Workflow: {flow_name}
Auto-generated from FloWhub on {datetime.now().isoformat()}
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
'''

        # XCom sidecar 설정
        if self.xcom_sidecar_image:
            header += f'''
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
PodDefaults.SIDECAR_CONTAINER.image = '{self.xcom_sidecar_image}'
'''

        # 공통 setup 스크립트
        if self.flow.repo:
            # repo가 있으면 git clone
            if self.flow.path:
                setup_script = f"git -c core.sshCommand=\"ssh -o StrictHostKeyChecking=no\"  clone {self.flow.repo} /repo\n                cd /repo/{self.flow.path}\n                cp -r . /app\n                mkdir -p /app/input"
            else:
                setup_script = f"git -c core.sshCommand=\"ssh -o StrictHostKeyChecking=no\"  clone {self.flow.repo} /app\n                mkdir -p /app/input"
        else:
            # ConfigMap에서 코드 복사 (base64 → 마운트된 파일 사용)
            setup_script = f"mkdir -p /app/input\n                cp {self.configmap_mount_path}/{self.workflow_filename} /app/{self.workflow_filename}"
            if self.extra_packages:
                for pkg_path in self.extra_packages:
                    if os.path.exists(pkg_path):
                        pkg_name = os.path.basename(pkg_path)
                        pkg_tar_name = f'{pkg_name}.tar.gz'
                        setup_script += f"\n                tar -xzf {self.configmap_mount_path}/{pkg_tar_name} -C /app"
                    else:
                        raise FileNotFoundError(
                            f"❌ EXTRA_PACKAGES에 지정된 패키지를 찾을 수 없습니다: {pkg_path}\n"
                            f"   현재 작업 디렉토리: {os.getcwd()}\n"
                            f"   절대 경로로 지정하거나, workflow 파일과 같은 위치에 패키지가 있는지 확인하세요."
                        )

        header += f'''# Common setup script
SETUP_SCRIPT = """{setup_script}"""

'''

        # DAG 정의
        default_args_str = self._format_dict(default_args, base_indent=1)
        description = self.flow.description or f'{flow_name} workflow'
        tags_str = repr(self.flow.tags) if self.flow.tags else f"['{self.flow.namespace}']"

        # Params 설정
        params_line = ""
        if self.flow.params:
            params_str = self._format_dict(self.flow.params, indent=1)
            params_line = f"\n    params={params_str},"

        dag_definition = f'''dag = DAG(
    '{self.dag_id}',
    default_args={default_args_str},
    description='{description}',
    schedule_interval={repr(schedule_interval)},
    catchup=False,
    tags={tags_str},{params_line}
)

'''

        # Common 설정 - ConfigMap 볼륨/마운트 추가
        if not self.flow.repo:
            # ConfigMap 볼륨/마운트를 common에 직접 포함 (값을 직접 인라인)
            configmap_volume = f"k8s.V1Volume(\n            name='code-volume',\n            config_map=k8s.V1ConfigMapVolumeSource(name='{self.configmap_name}')\n        )"
            configmap_volume_mount = f"k8s.V1VolumeMount(\n            name='code-volume',\n            mount_path='{self.configmap_mount_path}',\n            read_only=True\n        )"

            common_config_copy = self.common_config.copy()

            # 기존 volumes가 있으면 configmap_volume 추가, 없으면 새로 생성
            if 'volumes' in common_config_copy:
                existing_volumes_list = self._format_volumes_list(common_config_copy['volumes'])
                existing_volumes_list.append(configmap_volume)
                common_config_copy['volumes'] = '[\n        ' + ',\n        '.join(existing_volumes_list) + '\n    ]'
            else:
                common_config_copy['volumes'] = f'[\n        {configmap_volume}\n    ]'

            # volume_mounts는 common에 포함하지 않음 (task별로 병합해야 하므로)
            # 대신 base_volume_mounts 변수로 분리
            common_config_str = self._format_dict_with_raw_values(common_config_copy, indent=0)
            common_definition = f'''common = {common_config_str}

# Base volume mounts (ConfigMap mount)
base_volume_mounts = [
    {configmap_volume_mount}
]

'''
        else:
            common_config_str = self._format_dict(self.common_config, indent=0)
            common_definition = f'''common = {common_config_str}

'''

        # Task 정의들 (일반 workflow tasks만)
        tasks_code = "with dag:\n"
        for i, task in enumerate(self.flow.depend):
            task_code = self._generate_task_operator(task, is_first=(i == 0))
            tasks_code += task_code + "\n"

        # Failure handler task 추가 (trigger_rule='one_failed')
        if self.flow.on_failure:
            failure_task_code = self._generate_task_operator(
                self.flow.on_failure,
                is_first=True,
                trigger_rule='one_failed'
            )
            tasks_code += failure_task_code + "\n"

        # Task 의존성
        dependencies = self._generate_dependencies()
        if dependencies:
            tasks_code += "    # Task dependencies\n"
            for dep in dependencies:
                tasks_code += f"    {dep}\n"

        # Failure handler 의존성 추가
        if self.flow.on_failure:
            last_tasks = self._find_last_tasks()
            if last_tasks:
                tasks_code += f"    # Failure handler dependency\n"
                tasks_code += f"    [{', '.join(last_tasks)}] >> {self.flow.on_failure.name}\n"

        return header + dag_definition + common_definition + tasks_code

    def _generate_task_operator(self, task, is_first=False, trigger_rule=None):
        """KubernetesPodOperator 생성"""
        input_commands = self._generate_input_commands(task, is_first)

        # 커스텀 CLI 인자를 명령줄에 추가
        custom_args_str = self._build_custom_cli_args()

        arguments = f'''
                {{SETUP_SCRIPT}}
                {input_commands}
                cd /app && python3 -u {self.workflow_filename} --env {self.current_env} --id {{{{{{{{ dag_run.run_id }}}}}}}} --task {task.name}{custom_args_str} --input /app/input --output /airflow/xcom/return.json
            '''

        task_image = task.image if hasattr(task, 'image') and task.image else self.k8s_image

        tolerations_code = self._build_tolerations_code(task)
        node_selector_code = self._build_node_selector_code(task)
        volume_mounts_code = self._build_volume_mounts_code(task)
        container_resources_code = self._build_container_resources_code(task)
        sidecars_code = self._build_sidecars_code(task)

        # pool 추가
        pool_code = f"\n        pool='{task.pool}'," if hasattr(task, 'pool') and task.pool else ""

        # trigger_rule 추가
        trigger_rule_code = f"\n        trigger_rule='{trigger_rule}'," if trigger_rule else ""

        # failure handler는 retries=0 설정
        retries_code = "\n        retries=0," if trigger_rule == 'one_failed' else ""

        operator_code = f'''    {task.name} = KubernetesPodOperator(
        **common,
        task_id='{task.name}',
        image='{task_image}',{pool_code}{trigger_rule_code}{retries_code}{tolerations_code}{node_selector_code}{volume_mounts_code}{container_resources_code}{sidecars_code}
        arguments=[
            f\'\'\'{arguments}\'\'\'
        ],
    )'''

        return operator_code

    def _generate_input_commands(self, task, is_first):
        """Input 데이터 명령어 생성"""
        if is_first:
            return '\n'.join(["cat << 'EOF' > /app/input/0.json", '{{{{ params | tojson }}}}', 'EOF'])
        if not task.depend:
            return "echo '{{}}' >> /app/input/0.json"

        commands = [
            '\n'.join([f"cat << 'EOF' > /app/input/{i}.json", f'{{{{{{{{ ti.xcom_pull(task_ids=\"{dep.name}\") | tojson }}}}}}}}', 'EOF'])
            for i, dep in enumerate(task.depend)]
        return "\n                ".join(commands)

    def _build_tolerations_code(self, task):
        """Tolerations 코드 생성"""
        if not (hasattr(task, 'tolerations') and task.tolerations):
            return ""

        tolerations_items = []
        for tol in task.tolerations:
            tol_params = [f"{attr}='{getattr(tol, attr)}'"
                         for attr in ['key', 'operator', 'value', 'effect']
                         if hasattr(tol, attr) and getattr(tol, attr)]
            if tol_params:
                tolerations_items.append(f"k8s.V1Toleration({', '.join(tol_params)})")

        return f"\n        tolerations=[{', '.join(tolerations_items)}]," if tolerations_items else ""

    def _build_node_selector_code(self, task):
        """Node selector 코드 생성"""
        if hasattr(task, 'node_selector') and task.node_selector:
            return f"\n        node_selector={repr(task.node_selector)},"
        return ""

    def _build_volume_mounts_code(self, task, include_base=True):
        """Volume mounts 코드 생성

        Task별 volume_mounts가 있으면 base_volume_mounts와 병합.
        ConfigMap 마운트(code-volume)는 항상 포함되어야 함.

        Args:
            task: TaskDecorator
            include_base: base_volume_mounts를 포함할지 여부 (repo가 없을 때 True)
        """
        volume_mounts_items = []
        if hasattr(task, 'volume_mounts') and task.volume_mounts:
            for vm in task.volume_mounts:
                vm_params = []
                if hasattr(vm, 'name') and vm.name:
                    vm_params.append(f"name='{vm.name}'")
                if hasattr(vm, 'mount_path') and vm.mount_path:
                    vm_params.append(f"mount_path='{vm.mount_path}'")
                if hasattr(vm, 'readOnly'):
                    vm_params.append(f"read_only={vm.readOnly}")
                if vm_params:
                    volume_mounts_items.append(f"k8s.V1VolumeMount({', '.join(vm_params)})")

        # ConfigMap 마운트와 병합 (repo가 없을 때)
        if not self.flow.repo:
            if volume_mounts_items:
                return f"\n        volume_mounts=base_volume_mounts + [{', '.join(volume_mounts_items)}],"
            else:
                return f"\n        volume_mounts=base_volume_mounts,"

        # repo가 있을 때는 task별 volume_mounts만 사용
        return f"\n        volume_mounts=[{', '.join(volume_mounts_items)}]," if volume_mounts_items else ""

    def _build_custom_cli_args(self):
        """커스텀 CLI 인자를 명령줄 문자열로 변환

        예: {'mode': 'prod', 'debug': True} → ' --mode prod --debug'
        """
        if not self.custom_cli_args:
            return ""

        args_parts = []
        for key, value in self.custom_cli_args.items():
            # key를 CLI 형식으로 변환 (underscore → dash)
            cli_key = key.replace('_', '-')

            if isinstance(value, bool):
                if value:
                    args_parts.append(f"--{cli_key}")
            else:
                args_parts.append(f"--{cli_key} {value}")

        return ' ' + ' '.join(args_parts) if args_parts else ""

    def _build_container_resources_code(self, task):
        """Container resources 코드 생성"""
        resource_attrs = ['request_cpu', 'request_memory', 'limit_cpu', 'limit_memory', 'limit_gpu']
        if not any(hasattr(task, attr) for attr in resource_attrs):
            return ""

        requests = {}
        limits = {}
        if hasattr(task, 'request_cpu') and task.request_cpu:
            requests['cpu'] = task.request_cpu
        if hasattr(task, 'request_memory') and task.request_memory:
            requests['memory'] = task.request_memory
        if hasattr(task, 'limit_cpu') and task.limit_cpu:
            limits['cpu'] = task.limit_cpu
        if hasattr(task, 'limit_memory') and task.limit_memory:
            limits['memory'] = task.limit_memory
        if hasattr(task, 'limit_gpu') and task.limit_gpu:
            limits['nvidia.com/gpu'] = task.limit_gpu

        resource_requirements = f"k8s.V1ResourceRequirements(requests={repr(requests)}, limits={repr(limits)})"
        return f"\n        container_resources={resource_requirements},"

    def _build_sidecars_code(self, task) -> str:
        """full_pod_spec으로 사이드카 컨테이너 코드 생성"""
        if not (hasattr(task, 'sidecars') and task.sidecars):
            return ""

        # 사이드카 + 태스크 volume_mounts에서 참조된 볼륨명 수집
        referenced_volume_names = set()
        for sc in task.sidecars:
            for vm in sc.volume_mounts:
                referenced_volume_names.add(vm.name)
        if hasattr(task, 'volume_mounts') and task.volume_mounts:
            for vm in task.volume_mounts:
                referenced_volume_names.add(vm.name)

        # 사이드카 컨테이너 코드 생성
        sidecar_items = []
        for sc in task.sidecars:
            parts = [f"name='{sc.name}'", f"image='{sc.image}'"]

            if sc.volume_mounts:
                mounts = [
                    f"k8s.V1VolumeMount(name='{vm.name}', mount_path='{vm.mount_path}')"
                    for vm in sc.volume_mounts
                ]
                parts.append(f"volume_mounts=[{', '.join(mounts)}]")

            if sc.env:
                envs = [
                    f"k8s.V1EnvVar(name='{k}', value='{v}')"
                    for k, v in sc.env.items()
                ]
                parts.append(f"env=[{', '.join(envs)}]")

            if sc.cpu or sc.memory:
                requests = {}
                limits = {}
                if sc.cpu:
                    requests['cpu'] = sc.cpu
                    limits['cpu'] = sc.cpu
                if sc.memory:
                    requests['memory'] = sc.memory
                    limits['memory'] = sc.memory
                parts.append(
                    f"resources=k8s.V1ResourceRequirements("
                    f"requests={repr(requests)}, limits={repr(limits)})"
                )

            if sc.command:
                parts.append(f"command={repr(sc.command)}")
            if sc.args:
                parts.append(f"args={repr(sc.args)}")

            params_str = ',\n                        '.join(parts)
            sidecar_items.append(
                f"k8s.V1Container(\n                        {params_str}\n                    )"
            )

        containers_str = ',\n                    '.join(sidecar_items)

        # full_pod_spec에 포함할 emptyDir 볼륨 수집
        empty_dir_items = []
        for vol_name in sorted(referenced_volume_names):
            if vol_name in self.empty_dir_volumes:
                empty_dir_items.append(
                    f"k8s.V1Volume(\n                        name='{vol_name}',\n"
                    f"                        empty_dir=k8s.V1EmptyDirVolumeSource()\n"
                    f"                    )"
                )

        if empty_dir_items:
            volumes_str = ',\n                    '.join(empty_dir_items)
            spec_body = (
                f"containers=[\n                    {containers_str}\n                ],\n"
                f"                volumes=[\n                    {volumes_str}\n                ]"
            )
        else:
            spec_body = f"containers=[\n                    {containers_str}\n                ]"

        return (
            f"\n        full_pod_spec=k8s.V1Pod(\n"
            f"            spec=k8s.V1PodSpec(\n"
            f"                {spec_body}\n"
            f"            )\n"
            f"        ),"
        )

    def _encode_workflow(self):
        """전체 workflow.py 코드를 base64로 인코딩"""
        import sys
        workflow_file = sys.modules[self.flow.func.__module__].__file__
        with open(workflow_file, 'r', encoding='utf-8') as f:
            workflow_code = f.read()
        return base64.b64encode(workflow_code.encode()).decode()

    def _encode_package(self, package_path):
        """패키지를 tar.gz로 압축 후 base64 인코딩 (__pycache__ 등 제외)"""
        import tarfile
        import io

        exclude_patterns = {
            '__pycache__', '.pyc', '.pyo', '.pyd', '.so', '.git', '.gitignore',
            '.pytest_cache', '.mypy_cache', '.coverage', '.DS_Store', '.egg-info', 'dist', 'build'
        }
        exclude_extensions = ('.pyc', '.pyo', '.pyd', '.so')

        def should_exclude(name):
            """파일/디렉토리를 제외할지 판단"""
            parts = Path(name).parts
            return any(part in exclude_patterns or part.endswith(exclude_extensions) for part in parts)

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode='w:gz') as tar:
            package_name = os.path.basename(package_path)
            for root, dirs, files in os.walk(package_path):
                dirs[:] = [d for d in dirs if not should_exclude(os.path.join(root, d))]
                for file in files:
                    file_path = os.path.join(root, file)
                    if not should_exclude(file_path):
                        arcname = os.path.join(package_name, os.path.relpath(file_path, package_path))
                        tar.add(file_path, arcname=arcname)

        tar_buffer.seek(0)
        compressed_size = len(tar_buffer.getvalue())
        encoded = base64.b64encode(tar_buffer.read()).decode()
        print(f'📦 Package compressed: {compressed_size/1024:.2f} KB → Base64: {len(encoded)/1024:.2f} KB')
        return encoded

    def _find_last_tasks(self):
        """마지막 task들 (다른 task의 dependency가 아닌 task들) 찾기"""
        # 모든 일반 task (flow.depend는 on_failure를 포함하지 않음)
        all_tasks = {task.name for task in self.flow.depend}

        # dependency로 사용되는 task들 수집
        dependency_tasks = {
            dep.name
            for task in self.flow.depend
            if task.depend
            for dep in task.depend
        }

        # 마지막 task들 = 전체 - dependency로 사용되는 것들
        return sorted(all_tasks - dependency_tasks)

    def _generate_dependencies(self):
        """Task 의존성 생성"""
        dependencies = []
        for task in self.flow.depend:
            if task.depend:
                dep_names = [dep.name for dep in task.depend]
                if len(dep_names) == 1:
                    dependencies.append(f"{dep_names[0]} >> {task.name}")
                else:
                    deps_str = ', '.join(dep_names)
                    dependencies.append(f"[{deps_str}] >> {task.name}")
        return dependencies

    def _format_dict(self, d, indent=0, base_indent=0):
        """Dict를 Python 코드 형식으로 포맷팅"""
        return self._format_dict_with_raw_values(d, indent, raw_keys=set(), base_indent=base_indent)

    def _format_dict_with_raw_values(self, d, indent=0, raw_keys=None, base_indent=0):
        """Dict를 Python 코드 형식으로 포맷팅

        Args:
            d: 포맷팅할 dict
            indent: 들여쓰기 레벨
            raw_keys: 값을 문자열로 감싸지 않고 그대로 출력할 키들 (volumes, volume_mounts 등)
            base_indent: 기본 들여쓰기 (DAG 정의 내부에서 사용 시)
        """
        if not d:
            return "{}"

        if raw_keys is None:
            raw_keys = {'volumes', 'volume_mounts'}

        items = []
        for key, value in d.items():
            # raw_keys에 해당하는 키는 값을 그대로 출력 (이미 코드 문자열인 경우)
            if key in raw_keys and isinstance(value, str):
                items.append(f"'{key}': {value}")
            elif isinstance(value, str):
                items.append(f"'{key}': '{value}'")
            elif isinstance(value, datetime):
                items.append(f"'{key}': datetime({value.year}, {value.month}, {value.day})")
            elif isinstance(value, timedelta):
                items.append(f"'{key}': timedelta(minutes={int(value.total_seconds() / 60)})")
            elif isinstance(value, bool):
                items.append(f"'{key}': {value}")
            elif isinstance(value, (int, float)):
                items.append(f"'{key}': {value}")
            elif isinstance(value, dict):
                items.append(f"'{key}': {self._format_dict_with_raw_values(value, indent + 1, raw_keys, base_indent)}")
            elif isinstance(value, list):
                # volumes 리스트를 특별 처리
                if key == 'volumes':
                    volumes_str = self._format_volumes(value)
                    items.append(f"'{key}': {volumes_str}")
                else:
                    items.append(f"'{key}': {value}")
            else:
                items.append(f"'{key}': {repr(value)}")

        total_indent = base_indent + indent
        indent_str = '    ' * total_indent
        if len(items) <= 2:
            return '{' + ', '.join(items) + '}'
        else:
            return '{\n' + indent_str + '    ' + f',\n{indent_str}    '.join(items) + '\n' + indent_str + '}'

    def _format_volumes(self, volumes_list):
        """Volumes 리스트를 k8s.V1Volume 형태로 포맷팅 (한 줄)"""
        volumes_items = []
        for vol in volumes_list:
            if not (isinstance(vol, dict) and 'name' in vol):
                continue
            if 'persistent_volume_claim' in vol:
                pvc_claim = f"k8s.V1PersistentVolumeClaimVolumeSource(claim_name='{vol['persistent_volume_claim']}')"
                volumes_items.append(f"k8s.V1Volume(name='{vol['name']}', persistent_volume_claim={pvc_claim})")
            elif 'empty_dir' in vol:
                volumes_items.append(f"k8s.V1Volume(name='{vol['name']}', empty_dir=k8s.V1EmptyDirVolumeSource())")

        if len(volumes_items) == 0:
            return "[]"
        elif len(volumes_items) == 1:
            return f"[{volumes_items[0]}]"
        else:
            return "[\n        " + ",\n        ".join(volumes_items) + "\n    ]"

    def _format_volumes_list(self, volumes_list):
        """Volumes 리스트를 k8s.V1Volume 문자열 리스트로 반환 (여러 줄 포맷)"""
        volumes_items = []
        for vol in volumes_list:
            if not (isinstance(vol, dict) and 'name' in vol):
                continue
            if 'persistent_volume_claim' in vol:
                volumes_items.append(f"""k8s.V1Volume(
            name='{vol['name']}',
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name='{vol['persistent_volume_claim']}'
            )
        )""")
            elif 'empty_dir' in vol:
                volumes_items.append(f"""k8s.V1Volume(
            name='{vol['name']}',
            empty_dir=k8s.V1EmptyDirVolumeSource()
        )""")
        return volumes_items

    def _generate_configmap_yaml(self, output_dir):
        """ConfigMap YAML 파일 생성

        workflow 코드와 추가 패키지를 ConfigMap의 binaryData로 저장.
        Kubernetes ConfigMap의 최대 크기는 1MB이므로, 필요시 여러 ConfigMap으로 분할.
        """
        import yaml

        configmap_path = os.path.join(output_dir, f'{self.configmap_name}.yaml')

        # workflow 코드 읽기
        workflow_file = sys.modules[self.flow.func.__module__].__file__
        with open(workflow_file, 'r', encoding='utf-8') as f:
            workflow_code = f.read()

        # ConfigMap 데이터 준비
        data = {
            self.workflow_filename: workflow_code
        }

        binary_data = {}

        # 추가 패키지들을 tar.gz로 압축하여 binaryData에 추가
        if self.extra_packages:
            for pkg_path in self.extra_packages:
                if not os.path.exists(pkg_path):
                    raise FileNotFoundError(
                        f"❌ EXTRA_PACKAGES에 지정된 패키지를 찾을 수 없습니다: {pkg_path}\n"
                        f"   현재 작업 디렉토리: {os.getcwd()}\n"
                        f"   절대 경로로 지정하거나, workflow 파일과 같은 위치에 패키지가 있는지 확인하세요."
                    )
                pkg_name = os.path.basename(pkg_path)
                pkg_tar_name = f'{pkg_name}.tar.gz'
                pkg_b64 = self._encode_package(pkg_path)
                binary_data[pkg_tar_name] = pkg_b64

        # ConfigMap YAML 생성
        configmap = {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': self.configmap_name,
                'namespace': self.k8s_namespace,
                'labels': {
                    'app': self.dag_id,
                    'managed-by': 'flowhub'
                }
            },
            'data': data
        }

        if binary_data:
            configmap['binaryData'] = binary_data

        # YAML 파일 저장
        with open(configmap_path, 'w', encoding='utf-8') as f:
            yaml.dump(configmap, f, default_flow_style=False, allow_unicode=True)

        # ConfigMap 크기 체크 (경고)
        total_size = len(workflow_code)
        for key, value in binary_data.items():
            total_size += len(value)

        if total_size > 1024 * 1024:  # 1MB
            print(f'⚠️  Warning: ConfigMap size ({total_size / 1024:.1f}KB) exceeds 1MB limit.')
            print(f'   Consider using git repo option or splitting into multiple ConfigMaps.')

        return configmap_path
