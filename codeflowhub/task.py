from .base import BaseDecorator
from .model import Toleration, VolumeMount
from .action import Action

class TaskDecorator(BaseDecorator):
    request_cpu: str = '1'
    request_memory: str = '1Gi'
    limit_cpu: str = '1'
    limit_memory: str = '1Gi'
    limit_gpu: str
    image: str
    node_selector: dict
    tolerations: list[Toleration]
    volume_mounts: list[VolumeMount]

    def __init__(self, *args, cpu='1', memory='1Gi', gpu=None, image=None,
                 node_selector=None, tolerations: list[Toleration] = None,
                 volume_mounts: list[VolumeMount] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_resource(cpu, memory, gpu)
        self.image = image
        self.node_selector = node_selector or {}
        self.tolerations = tolerations or []
        self.volume_mounts = volume_mounts or []
        self._is_flowhub_task = True

    def set_resource(self, cpu, memory, gpu):
        if cpu:
            self.request_cpu = self.limit_cpu = cpu
        if memory:
            self.request_memory = self.limit_memory = memory
        if gpu:
            self.limit_gpu = gpu

    def __call__(self, *args, **kwargs) -> 'TaskDecorator':
        has_action = any(isinstance(arg, Action) for arg in args)
        has_task = any(isinstance(arg, BaseDecorator) and not isinstance(arg, Action) for arg in args)

        if has_action:
            return self._handle_action_mode(args)

        if has_task:
            return self._handle_build_mode(args)

        return self._handle_execution_mode(args, kwargs)

    def _handle_action_mode(self, args):
        """Action 모드: 의존성 수집"""
        action_obj = None
        dependencies = []

        for arg in args:
            if isinstance(arg, Action):
                action_obj = arg
            elif isinstance(arg, BaseDecorator):
                dependencies.append(arg)

        self.depend = dependencies

        if action_obj and hasattr(action_obj, 'called_tasks'):
            if self not in action_obj.called_tasks:
                action_obj.called_tasks.append(self)
            self._action_obj = action_obj

        return self

    def _handle_build_mode(self, args):
        """빌드 모드: TaskDecorator 의존성 수집"""
        action_obj = None
        dependencies = []

        for arg in args:
            if isinstance(arg, BaseDecorator):
                dependencies.append(arg)
                if hasattr(arg, '_action_obj') and arg._action_obj:
                    action_obj = arg._action_obj

        self.depend = dependencies

        if action_obj and hasattr(action_obj, 'called_tasks'):
            if self not in action_obj.called_tasks:
                action_obj.called_tasks.append(self)
            self._action_obj = action_obj

        return self

    def _handle_execution_mode(self, args, kwargs):
        """일반 실행 모드: dict 인자 처리"""
        if len(args) > 1:
            merged = {}
            for arg in args:
                if isinstance(arg, dict):
                    merged.update(arg)
            self._inject_task_name(merged)
            return super().__call__(merged, **kwargs)

        if args and isinstance(args[0], dict):
            input_data = args[0].copy()
            self._inject_task_name(input_data)
            return super().__call__(input_data, **kwargs)

        return super().__call__(*args, **kwargs)

    def _inject_task_name(self, data):
        """env에 task 이름 주입"""
        if 'env' in data and isinstance(data['env'], dict):
            data['env'] = {**data['env'], 'task': self.name}