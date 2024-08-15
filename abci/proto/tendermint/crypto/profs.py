from dataclasses import dataclass, field

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from typing_extensions import Annotated


@dataclass
class ProofOp(BaseMessage):
    type: Annotated[str, Field(1)]
    key: Annotated[bytes, Field(2)]
    data: Annotated[bytes, Field(3)]


@dataclass
class ProofOps(BaseMessage):
    ops: Annotated[list[ProofOp], Field(1)] = field(default_factory=list)
