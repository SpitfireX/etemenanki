from components import Component

from typing import Tuple, Optional
from collections.abc import Sequence
from io import RawIOBase
from uuid import UUID, uuid4
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

    def __init__(self, components: Sequence[Component], container_type: str, dimensions: Tuple[int, int], uuid: Optional[UUID] = None, base_uuids: Tuple[Optional[UUID], Optional[UUID]] = (None, None)) -> None:
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
            UUID4 for the container. If empty a new UUID will be generated.
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
        self.uuid = uuid if uuid else uuid4()
        self.base_uuids = base_uuids


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
        f.write('1.0\t'.encode('ascii')) # version
        f.write(self.container_type[0].encode('ascii')) # container family
        f.write(self.container_type[1].encode('ascii')) # container class
        f.write(self.container_type[2].encode('ascii')) # container type
        f.write('\n'.encode('ascii')) # LF

        f.write(str(self.uuid).encode('ascii')) # uuid as ASCII (36 bytes)
        f.write('\n\x04\0\0'.encode('ascii')) # LF EOT 0 0

        # components meta
        f.write(pack('B', len(self.components))) #allocated
        f.write(pack('B', len(self.components))) #used

        f.write(bytes(6)) # padding

        # dimensions
        f.write(pack('<q', self.dimensions[0])) # dim1
        f.write(pack('<q', self.dimensions[1])) # dim2

        # referenced base layers
        if self.base_uuids[0]:
            s = str(self.base_uuids[0]).encode('ascii')
            assert len(s) == 36, "UUID must be 36 bytes long"
            f.write(s)
        else:
            f.write(bytes(36)) # base1_uuid + padding
        f.write(bytes(4)) # padding
        
        if self.base_uuids[1]:
            s = str(self.base_uuids[1]).encode('ascii')
            assert len(s) == 36, "UUID must be 36 bytes long"
            f.write(s)
        else:
            f.write(bytes(36)) # base2_uuid + padding
        f.write(bytes(4)) # padding

        # file offsets
        self.offsets = [data_start(len(self.components))]
        for i, c in enumerate(self.components[1:], start=1):
            self.offsets[i] = align_offset(self.offsets[i-1] + c.bytelen())

        sizes = [c.bytelen() for c in self.components]

        # write BOM entries
        for c, o, s in zip(self.components, self.offsets, sizes):
            c.write_bom(f, o, s)


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
        for component in self.components:
            component.write(f)
