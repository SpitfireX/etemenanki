import numpy as np

from .varint import encode_varint

from abc import ABC, abstractmethod
from typing import Tuple, Optional, Iterable, Any
from io import RawIOBase
from struct import pack

BLOCKSIZE = 16

class Component(ABC):
    """Abstract base class for Ziggurat data components."""

    def __init__(self, component_type: int, mode: int, name: str, params: Tuple[Optional[int], Optional[int]]) -> None:
        """
        Instantiates a new Component object with all necessary data.

        Parameters
        ----------
        component_type : int
        mode : int
        name : str
            Name of the compenent, maximum lenght = 12.
        params : Tuple[Optional[int], Optional[int]]
            Two arbitrary parameters set by the spec or the user.

        Returns
        -------
        A new (immutable) Component object.
        """

        assert len(name.encode('ascii')) <= 12
        self.component_type = component_type
        self.mode = mode
        self.name = name
        self.params = params


    def write_bom(self, f: RawIOBase, offset: int, size: int) -> None:
        """
        Writes the BOM entry for the component to f.

        Parameters
        ----------
        f : RawIOBase
            A raw binary IO stream(-like object).
        offset: int
            The offset of the component within the container file.
        size: int
            The size in bytes of the component.
        """

        name = self.name.encode('ascii')
        assert len(name) <= 12

        f.write(pack('B', 1))
        f.write(pack('B', self.component_type))
        f.write(pack('B', self.mode))
        f.write(name.ljust(13, b'\0'))
        f.write(pack('<q', offset))
        f.write(pack('<q', size))
        f.write(pack('<q', self.params[0] if self.params[0] else 0))
        f.write(pack('<q', self.params[1] if self.params[1] else 0))


    @abstractmethod
    def bytelen(self) -> int:
        """Returns the length of the componen int bytes."""
        pass


    @abstractmethod
    def write(self, f: RawIOBase) -> None:
        """
        Writes the complete component to f.

        Parameters
        ----------
        f : RawIOBase
            A raw binary IO stream(-like object).
        """
        pass


class Vector(Component):
    
    def __init__(self, items: Iterable[Any], name:str, n: int, d: int = 1,):
        super().__init__(
            0x04,
            0x00,
            name,
            (n, d)
        )
        self.n = n
        self.d = d
        self.data = np.atleast_2d(np.array(items, dtype=np.int64))
        self.data.shape = (d, n)

    
    def bytelen(self):
        return self.n * self.d * 8 

    
    def write(self, f):

        for i in range(self.n):
            for j in range(self.d):
                f.write(pack('<q', self.data[j][i]))


class VectorDelta(Component):

    def __init__(self, items: Iterable[Any], name:str, n: int, d: int = 1,):
        super().__init__(
            0x04,
            0x02,
            name,
            (n, d)
        )
        self.n = n
        self.d = d
        data = np.atleast_2d(np.array(items, dtype=np.int64))
        data.shape = (n, d)

        self.data = data # TODO entfernen
        # compress data

        m = int((n - 1) / BLOCKSIZE) + 1
        delta_start = m*8

        # VarInt encoded blocks
        blocks = []

        for i in range(0, n, BLOCKSIZE):
            block = data[i : i+BLOCKSIZE]
            delta = np.empty(block.shape, dtype=np.int64)
            delta[0] = np.copy(block[0])

            cols = []

            for j in range(d):
                for i in range(1, len(block)):
                    delta[i][j] = block[i][j] - block[i-1][j]

                row = delta[:, j]
                varints = []
                for i in row:
                    varints.append(encode_varint(i))

                cols.append(b''.join(varints))

            blocks.append(b''.join(cols))

        assert len(blocks) == m

        # Sync offsets
        sync = [delta_start]
        for i, b in enumerate(blocks[:-1], start=1):
            sync.append(sync[i-1] + len(b))

        assert len(sync) == m

        self.encoded = b''.join(pack('<q', s) for s in sync) +\
            b''.join(blocks)

    
    def bytelen(self):
        return len(self.encoded)

    
    def write(self, f):
        f.write(self.encoded)
