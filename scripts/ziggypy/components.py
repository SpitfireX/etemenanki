from abc import ABC, abstractmethod
from typing import Tuple, Optional
from io import RawIOBase
from struct import pack

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
        f.write(name.ljust(13))
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
