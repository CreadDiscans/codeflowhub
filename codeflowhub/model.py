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