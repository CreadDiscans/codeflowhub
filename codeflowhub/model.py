from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Toleration:
    key: str
    operator: str
    effect: str
    value: Optional[str] = None

@dataclass
class VolumeMount:
    name: str
    mount_path: str
    readOnly: bool = False

@dataclass
class Volume:
    name: str
    persistent_volume_claim: Optional[str] = None
    empty_dir: Optional[dict] = None

# Design Ref: §2.1 — K8s Secret을 env로 주입 (Airflow Secret('env', ...) 매핑)
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

@dataclass
class SidecarContainer:
    name: str
    image: str
    volume_mounts: list = field(default_factory=list)   # list[VolumeMount]
    env: dict = field(default_factory=dict)              # {key: value}
    cpu: Optional[str] = None
    memory: Optional[str] = None
    command: Optional[list] = None
    args: Optional[list] = None
    share_process_namespace: bool = False