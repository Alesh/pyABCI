from dataclasses import dataclass

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from typing_extensions import Annotated


@dataclass
class Timestamp(BaseMessage):
    """ Timestamp message

    Attributes:
        seconds: Represents seconds of UTC time since Unix epoch 1970-01-01T00:00:00Z
        nanos: Non-negative fractions of a second at nanosecond resolution.
    """
    seconds: Annotated[int, Field(1)] = 0
    nanos: Annotated[int, Field(2)] = 0
