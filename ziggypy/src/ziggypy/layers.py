from abc import ABC
from typing import Optional, Iterable
from uuid import UUID, uuid4
from io import RawIOBase

from .container import Container
from .components import *


class Layer(ABC):

    def __init__(self, n: int, partition: Iterable[int], uuid: UUID):
        self.n = n
        self.partition = list(partition)
        self.uuid = uuid

        assert len(self.partition) >= 2, "A layer needs at least one partition spanning all positions, i.e. (0, n)"


    def write(self, f: RawIOBase):
        self.container.write(f)


class PrimaryLayer(Layer):

    def __init__(self, n: int, partition: Sequence[int], uuid: Optional[UUID] = None):
        
        super().__init__(n, partition, uuid if uuid else uuid4())

        p_vec = Vector(self.partition, "Partition", len(self.partition))

        self.container = Container(
            (p_vec,),
            "ZLp",
            (self.n, 0),
            self.uuid
        )


class SegmentationLayer(Layer):

    def __init__(self, n: int, partition: Sequence[int], ranges: Iterable[Tuple[int, int]], uuid: Optional[UUID] = None):

        super().__init__(n, partition, uuid if uuid else uuid4())

        p_vec = Vector(self.partition, "Partition", len(self.partition))

        ranges = np.atleast_2d(np.array(ranges, dtype=np.int64))
        ranges.shape = (n, 2)

        range_stream = VectorDelta(ranges, "RangeStream", n, d = 2)

        start_sort = IndexCompressed(ranges, "StartSort", n, sorted=True)

        end_sort = IndexCompressed(ranges[:,[1,0]], "EndSort", n, sorted=False)

        self.container = Container(
            (
                p_vec,
                range_stream,
                start_sort,
                end_sort
            ),
            "ZLs",
            (self.n, 0),
            self.uuid
        )
