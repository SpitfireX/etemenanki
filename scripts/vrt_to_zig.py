#!/usr/bin/env python3

import argparse

from ziggypy.varint import encode_varint
from ziggypy.container import Container
from ziggypy.components import *

from pathlib import Path
from uuid import UUID
from struct import pack
from itertools import chain, accumulate, islice, groupby

from fnvhash import fnv1a_64

def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch

parser = argparse.ArgumentParser(description='Script to convert a VRT file to a ziggurat basic layer')
parser.add_argument('input', type=Path,
                    help='The VRT file to convert')
parser.add_argument('-o', type=Path, required=False, dest="output",
                    help='The output directory for the Ziggurat data store. Default is input filename without extension')
parser.add_argument('-f', '--force', action='store_true',
                    help='Force overwrite output if directory already exists')
parser.add_argument('-u', '--uncompressed', action='store_true',
                    help='Write all components uncompressed (storage mode 0x00)')

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
print('Scanning VRT file...')
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
p = args.output / (str(base_uuid) + '.zigl')
print(f'Writing Base Layer file {p}')
f = p.open(mode='wb')

# partition vector:
# no partition = 1 partition spanning the entire corpus
# with boundaries (0, clen)
partitions = [0, clen]

p_vec = Vector(partitions, 'Partition', len(partitions))

primary_layer = Container(
    (p_vec,),
    'ZLp',
    (clen, 0),
    base_uuid
)

primary_layer.write(f)

f.close()

### Process VRT

print('Processing VRT...')

## gather data

corpus = []
pcount = 0

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

## data structures for Plain String Variable for tokens

# build StringData [string]
print('Building StringData')
string_data = b''.join(corpus[0])

# build OffsetStream [offset_to_next_string]
print('Building OffsetStream')
offset_stream = list(accumulate(chain([0], corpus[0]), lambda x, y: x + len(y)))
if args.uncompressed:
    offset_stream = [pack('<q', o) for o in offset_stream]
    offset_stream = b''.join(offset_stream)
else:
    m = int((len(offset_stream) - 1) / 16) + 1
    delta_start = m*8

    delta_stream = batched(offset_stream, 16)
    delta_stream = (chain((block[0],), (x2 - x1 for x1, x2 in zip(block[:-1], block[1:]))) for block in delta_stream)
    delta_stream = [b''.join(encode_varint(i) for i in block) for block in delta_stream]
    assert m == len(delta_stream)

    sync_stream = accumulate(chain((delta_start,), delta_stream[:-1]), lambda x, y: x + len(y))
    sync_stream = [pack('<q', o) for o in sync_stream]
    assert m == len(sync_stream)

    offset_stream = b''.join(sync_stream) + b''.join(delta_stream)

# build StringHash [(hash, cpos)]
print('Building StringHash')
string_pairs = [(fnv1a_64(s), i) for i, s in enumerate(corpus[0])]
string_pairs.sort(key=lambda x: x[0])

if args.uncompressed:
    string_hash = []
    for pair in string_pairs:
        string_hash.extend(pair)

    string_hash = [pack('<Q', x) for x in string_hash]
    string_hash = b''.join(string_hash)
else:
    blocks = []
    newblk = []
    for key, values in groupby(string_pairs, key=lambda x: x[0]):
        if len(newblk) < 16:
            newblk.extend(values)
        else:
            blocks.append(newblk)
            newblk = list(values)

    o = len(string_pairs) - (len(blocks) * 16)
    r = len(blocks) * 16
    mr = int((r - 1) / 16) + 1
    delta_start = mr*8+8

    assert mr == len(blocks)
    
    print(f'Compressed Index:')
    print(f'\t{len(string_pairs)} total items')
    print(f'\t{r} regular items, {o} overflow items')
    print(f'\t{len(blocks)} sync blocks')

    packed_blocks = []
    block_keys = []

    for b in blocks:
        bo = encode_varint(len(b) - 16)

        keys = [k for k, _ in (groupby(b, key=lambda x: x[0]))]
        block_keys.append(keys[0])
        keys = chain((keys[0],), (x2 - x1 for x1, x2 in zip(keys[:-1], keys[1:])))
        keys = b''.join(encode_varint(x) for x in keys)

        pos = [v for _, v in b]
        pos = chain((pos[0],), (x2 - x1 for x1, x2 in zip(pos[:-1], pos[1:])))
        pos = b''.join(encode_varint(x) for x in pos)
        
        packed_blocks.append(bo + keys + pos)

    assert mr == len(packed_blocks)
    assert mr == len(block_keys)
    
    blk_offsets = accumulate(chain((delta_start,), packed_blocks[:-1]), lambda x, y: x + len(y))
    sync_stream = []
    for bk, o in zip(block_keys, blk_offsets):
        sync_stream.append(pack('<Q', bk)) # Q because hash value
        sync_stream.append(pack('<q', o))

    string_hash = pack('<q', r) + b''.join(sync_stream) + b''.join(packed_blocks)


## data structures for Indexed String Variable for POS tags

pos_lex = set(corpus[1])


### write PlainString variable container for Tokens
p = args.output / (str(tok_uuid) + '.zigv')
print(f'Writing Plain String Layer file {p}')
f = p.open(mode="wb")

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
        0x00 if args.uncompressed else 0x02,
        offsets[1],
        len(offset_stream),
        clen + 1,
        1
    ),
    bom_entry(
        0x06,
        'StringHash',
        0x00 if args.uncompressed else 0x01,
        offsets[2],
        len(string_hash),
        clen,
        2
    )
)

## write components

# "StringData" StringList

f.write(string_data)

# "OffsetStream" Vector:delta

f.write(bytes(offsets[1] - f.tell())) # extra padding for alignment
f.write(offset_stream)

# "StringHash" Index:comp

f.write(bytes(offsets[2] - f.tell())) # extra padding for alignment
f.write(string_hash)

f.close()
