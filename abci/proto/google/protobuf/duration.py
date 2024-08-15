from dataclasses import dataclass

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from typing_extensions import Annotated


@dataclass
class Duration(BaseMessage):
    """ Duration message

    Attributes:
        seconds: Signed seconds of the span of time. Must be from -315,576,000,000 to +315,576,000,000 inclusive.
        nanos: Signed fractions of a second at nanosecond resolution of the span of time.
    """
    seconds: Annotated[int, Field(1)] = 0
    nanos: Annotated[int, Field(2)] = 0
