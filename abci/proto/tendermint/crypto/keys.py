from dataclasses import dataclass
from typing import Optional

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from pure_protobuf.one_of import OneOf
from typing_extensions import Annotated


@dataclass
class PublicKey(BaseMessage):
    value = OneOf[Optional[BaseMessage]]()
    which_one = value.which_one_of_getter()

    ed25519: Annotated[bytes, Field(1)] = None
    secp256k1: Annotated[bytes, Field(2)] = None

