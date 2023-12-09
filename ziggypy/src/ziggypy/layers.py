from abc import ABC
from typing import Optional, Iterable
from uuid import UUID, uuid4
from io import RawIOBase

from .container import Container
from .components import *


class Layer(ABC):

    def __init__(self, n: int, uuid: UUID):
        self.n = n
        self.uuid = uuid


    def write(self, f: RawIOBase):
        self.container.write(f)


class PrimaryLayer(Layer):

    def __init__(self, n: int, uuid: Optional[UUID] = None, comment: str = ""):
        
        super().__init__(n, uuid if uuid else uuid4())

        self.container = Container(
            (),
            "ZLp",
            (self.n, 0),
            self.uuid,
            comment=comment,
        )


class SegmentationLayer(Layer):

    def __init__(self, base_layer: Layer, n: int, ranges: Iterable[Tuple[int, int]], uuid: Optional[UUID] = None, compressed: bool = True, comment: str = ""):

        super().__init__(n, uuid if uuid else uuid4())

        ranges = np.atleast_2d(np.array(ranges, dtype=np.int64))
        ranges.shape = (n, 2)

        range_stream = VectorDelta(ranges, "RangeStream", n, d = 2)

        range_start_index = [(b, a[0]) for (a,b) in np.ndenumerate(ranges[:,0])]
        range_end_index = [(b, a[0]) for (a,b) in np.ndenumerate(ranges[:,1])]
        
        if compressed:
            start_sort = IndexCompressed(range_start_index, "StartSort", n, sorted=True)
            end_sort = IndexCompressed(range_end_index, "EndSort", n, sorted=False)
        else:
            start_sort = Index(range_start_index, "StartSort", n, sorted=True)
            end_sort = Index(range_end_index, "EndSort", n, sorted=False)

        self.container = Container(
            (
                range_stream,
                start_sort,
                end_sort
            ),
            "ZLs",
            (self.n, 0),
            self.uuid,
            base_uuids=(base_layer.uuid, None),
            comment=comment,
        )
