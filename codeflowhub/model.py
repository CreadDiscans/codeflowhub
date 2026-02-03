from dataclasses import dataclass
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
    persistent_volume_claim: str