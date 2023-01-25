#!/usr/bin/env python3

import argparse

from varint import encode_varint

from pathlib import Path
from uuid import UUID
from struct import pack
from itertools import chain, accumulate

from fnvhash import fnv1a_64

parser = argparse.ArgumentParser(description='Script to convert a VRT file to a ziggurat basic layer')
parser.add_argument('input', type=Path,
                    help='The VRT file to convert')
parser.add_argument('-o', type=Path, required=False, dest="output",
                    help='The output directory for the Ziggurat data store. Default is input filename without extension')
parser.add_argument('-f', '--force', action='store_true',
                    help='Force overwrite output if directory already exists')

args = parser.parse_args()

# output file handling

if not args.output:
    args.output = Path(args.input.stem)

if args.output.exists() and not args.force:
    print(f"Output directory {args.output} exists, aborting.")
    exit()
else:
    if not args.output.exists():
        args.output.mkdir()

# A datastore consists of container files, which all have a UUID v4.
# Container files can be layer files and variables assigned to them.
# A datastore is built up from the bottom beginning with a primary layer that
# provides a global index of corpus positions (cpos), its variables, and additional
# layers that can reference layers below them.
# All these containers are linked via UUIDs.

# static uuids for now to make testing easier
base_uuid = UUID('b764b867-cac4-4329-beda-9c021c5184d7') # uuid of base layer container
tok_uuid = UUID('b7887880-e234-4dd0-8d6a-b8b99397b030') # uuid of first P-attr (token stream)
pos_uuid = UUID('634575cf-43c2-4a7e-b239-4e0ce2ecb394') # uuid of second P-attr (pos tags)

### Scan VRT file for number of tokens
clen = 0 # length of corpus, first to be determined by scanning the VRT file

with args.input.open() as f:
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                clen += 1
print(f'Found input file with {clen} corpus positions')

BOM_START = 160
LEN_BOM_ENTRY = 48

def align_offset(o):
    if o % 8 > 0:
        return o + (8 - (o % 8))
    else:
        return o

def data_start(clen):
    return BOM_START + (clen * LEN_BOM_ENTRY)

def bom_entry(type, name, mode, offset,
            size, param1, param2):
    assert len(name.encode('ascii')) <= 12
    return pack('B', 1) +\
        pack('B', type) +\
        pack('B', mode) +\
        name.encode('ascii').ljust(13) +\
        pack('<q', offset) +\
        pack('<q', size) +\
        pack('<q', param1) +\
        pack('<q', param2)

def write_container_header(f, ctype, uuid, dimensions, 
                           base1_uuid, base2_uuid, *bom_entries):
    # consts
    f.write('Ziggurat'.encode('ascii')) # magic
    f.write('1.0\t'.encode('ascii')) # version
    f.write(ctype[0].encode('ascii')) # container family
    f.write(ctype[1].encode('ascii')) # container class
    f.write(ctype[2].encode('ascii')) # container type
    f.write('\n'.encode('ascii')) # LF

    f.write(str(uuid).encode('ascii')) # uuid as ASCII (36 bytes)
    f.write('\n\x04\0\0'.encode('ascii')) # LF EOT 0 0

    # components meta
    f.write(pack('B', len(bom_entries))) #allocated
    f.write(pack('B', len(bom_entries))) #used

    f.write(bytes(6)) # padding

    # dimensions
    f.write(pack('<q', dimensions[0])) # dim1
    f.write(pack('<q', dimensions[1])) # dim2

    # referenced base layers
    if base1_uuid:
        s = str(base1_uuid).encode('ascii')
        assert len(s) == 36, "UUID must be 36 bytes long"
        f.write(s)
    else:
        f.write(bytes(36)) # base1_uuid + padding
    f.write(bytes(4)) # padding
    
    if base2_uuid:
        s = str(base2_uuid).encode('ascii')
        assert len(s) == 36, "UUID must be 36 bytes long"
        f.write(s)
    else:
        f.write(bytes(36)) # base2_uuid + padding
    f.write(bytes(4)) # padding

    # write BOM entries
    for entry in bom_entries:
        f.write(entry)

### Write Base Layer container

f = (args.output / (str(base_uuid) + '.zigl')).open(mode="wb")

## write header
write_container_header(f,
    'ZLp',
    base_uuid,
    (clen, 0),
    None,
    None,
    bom_entry(
        0x4,
        'Partition',
        0x0,
        data_start(1), # offset
        16, # size
        clen,
        1
    )
)

## write components

# Partition vector (min size 2)

# no partition = 1 partition spanning the entire corpus
# with boundaries (0, clen)

f.write(pack('<q', 0))
f.write(pack('<q', clen))

f.close()

### Process VRT

## gather data

corpus = []

with args.input.open() as f:
    # find number of p attrs
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                pcount = len(line.split())
                break
    
    print(f'Corpus has {pcount} p-attrs')
    corpus = [[] for _ in range(pcount)]
    f.seek(0) # reset file to beginning

    for line in f:
        if not line.startswith("<"):
            if line.strip():
                pattrs = line.split()
                for i, attr in enumerate(pattrs):
                    corpus[i].append((attr + '\0').encode('utf-8'))

# double check dimensions
for attr in corpus:
    assert len(attr) == clen

# build StringData
string_data = b''.join(corpus[0])

# build OffsetStream
offset_stream = list(accumulate(chain([0], corpus[0]), lambda x, y: x + len(y)))
offset_stream = [pack('<q', o) for o in offset_stream]
offset_stream = b''.join(offset_stream)

# build StringHash

string_hash = [(fnv1a_64(s), i) for i, s in enumerate(corpus[0])]
string_hash.sort(key=lambda x: x[0])
string_hash = list(sum(string_hash, ()))
string_hash = [pack('<Q', x) for x in string_hash]
string_hash = b''.join(string_hash)

### write PlainString variable container for Tokens

f = (args.output / (str(tok_uuid) + '.zigv')).open(mode="wb")

## write header

nbom = 3

offsets = [data_start(nbom)]
offsets.append(align_offset(offsets[0] + len(string_data)))
offsets.append(align_offset(offsets[1] + len(offset_stream)))

print('offset table:')
for i, o in enumerate(offsets):
    print(f'\tcomponent {i+1}\t{hex(o)}')

write_container_header(f,
    'ZVc',
    tok_uuid,
    (clen, 0),
    base_uuid,
    None,
    bom_entry(
        0x02,
        'StringData',
        0x00,
        offsets[0],
        len(string_data),
        clen,
        0
    ),
    bom_entry(
        0x04,
        'OffsetStream',
        0x00,
        offsets[1],
        len(offset_stream),
        clen + 1,
        1
    ),
    bom_entry(
        0x06,
        'StringHash',
        0x00,
        offsets[2],
        len(string_hash),
        clen,
        2
    )
)

## write components

# "StringData" StringList

f.write(string_data)

# "OffsetStream" Vector (TODO add compression)

f.write(bytes(offsets[1] - f.tell())) # extra padding for alignment
f.write(offset_stream)

# "StringHash" Index (TODO add compression)

f.write(bytes(offsets[2] - f.tell())) # extra padding for alignment
f.write(string_hash)

f.close()
