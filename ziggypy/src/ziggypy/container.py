from .components import Component

from typing import Tuple, Optional
from collections.abc import Sequence
from io import RawIOBase
from uuid import UUID
from struct import pack


BOM_START: int = 160
LEN_BOM_ENTRY: int = 48


def data_start(cn: int) -> int:
    """
    Aligns the offset o to an 8-byte boundary by adding padding.

    Parameters
    ----------
    cn : int
        The number of components in the BOM.

    Returns
    -------
    int
        The offset within the container file where the data section starts.  
    """
    return BOM_START + (cn * LEN_BOM_ENTRY)


def align_offset(o: int) -> int:
    """
    Aligns the offset o to an 8-byte boundary by adding padding.

    Parameters
    ----------
    o : int
        An arbitrary offset.

    Returns
    -------
    int
        o + necessary padding 
    """
    
    if o % 8 > 0:
        return o + (8 - (o % 8))
    else:
        return o


class Container():
    """Instances of the Container class represent a Ziggurat container file."""

    def __init__(self, components: Sequence[Component], container_type: str, dimensions: Tuple[int, int], uuid: UUID, base_uuids: Tuple[Optional[UUID], Optional[UUID]] = (None, None), comment: str = "") -> None:
        """
        Instantiates a new Container object with all necessary data.

        Parameters
        ----------
        components : Sequence[Component]
            A sequence of components in this container.
        container_type : str
            Ziggurat container type consisting of 3 characters, e.g. "ZVc".
        dimensions : Tuple[int, int]
            Two integers describing the dimensions of the Container.
        uuid : Optional[UUID] = None
            UUID4 for the container.
        base_uuids : Tuple[Optional[UUID]
            UUID4s of the base layers referenced by this container.

        Returns
        -------
        A new (immutable) Container object.
        """

        assert len(container_type) == 3, "Ziggurat container type must be 3 chars long"
        
        self.components = components
        self.container_type = container_type
        self.dimensions = dimensions
        self.uuid = uuid
        self.base_uuids = base_uuids

        self.comment = comment.encode('utf-8')
        self.comment += " encoded using ZiggyPy".encode('utf-8')
        assert len(self.comment) < 72, "Comment exceeding maximum length"


    def write_header(self, f: RawIOBase) -> None:
        """
        Writes the file header of container file to f.

        Parameters
        ----------
        f : RawIOBase
            A raw binary IO stream(-like object).
        """

        # consts
        f.write('Ziggurat'.encode('ascii')) # magic
        f.write('1.0'.encode('ascii')) # version
        f.write(self.container_type[0].encode('ascii')) # container family
        f.write(self.container_type[1].encode('ascii')) # container class
        f.write(self.container_type[2].encode('ascii')) # container type

        # components meta
        f.write(pack('B', len(self.components))) #allocated
        f.write(pack('B', len(self.components))) #used

        # container UUID
        f.write(self.uuid.bytes)

        # referenced base layers
        if self.base_uuids[0]:
            u = self.base_uuids[0].bytes
        else:
            u = bytes(16) # padding
        assert len(u) == 16, "UUID must be 16 bytes long"
        f.write(u)
        
        if self.base_uuids[1]:
            u = self.base_uuids[1].bytes
        else:
            u = bytes(16) # padding
        assert len(u) == 16, "UUID must be 16 bytes long"
        f.write(u)

        # dimensions
        f.write(pack('<q', self.dimensions[0])) # dim1
        f.write(pack('<q', self.dimensions[1])) # dim2

        # extensions
        f.write(bytes(8)) # unused for now

        # comment
        f.write(self.comment.ljust(72, b"\0"))

        # BOM
        # file offsets
        self.offsets = [data_start(len(self.components))]
        for i, c in enumerate(self.components[:-1], start=1):
            self.offsets.append(align_offset(self.offsets[i-1] + c.bytelen()))

        if len(self.components) > 0:
            print(f'offset table for container {self.uuid}:')
        for i, (o, c) in enumerate(zip(self.offsets, self.components)):
            print(f'\tcomponent {i+1} "{c.name}"\t{hex(o)}\tlen({c.bytelen()})')

        # write BOM entries
        for c, o in zip(self.components, self.offsets):
            c.write_bom(f, o, c.bytelen())


    def write(self, f: RawIOBase) -> None:
        """
        Writes the complete container to f.

        Parameters
        ----------
        f : RawIOBase
            A raw binary IO stream(-like object).
        
        See Also
        --------
        write_header : Writes only the file header, used by this method.
        """

        self.write_header(f)
        for component, offset in zip(self.components, self.offsets):
            f.write(bytes(offset - f.tell())) # extra padding for alignment
            component.write(f)
