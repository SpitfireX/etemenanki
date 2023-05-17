#!/usr/bin/env python3

import argparse
import xml.parsers.expat as expat

from ziggypy.components import *
from ziggypy.layers import *
from ziggypy.variables import *

from pathlib import Path

parser = argparse.ArgumentParser(description="Script to convert a VRT file to a ziggurat basic layer")
parser.add_argument("input", type=Path,
                    help="The VRT file to convert")
parser.add_argument("-o", type=Path, required=False, dest="output",
                    help="The output directory for the Ziggurat data store. Default is input filename without extension")
parser.add_argument("-f", "--force", action="store_true",
                    help="Force overwrite output if directory already exists")
parser.add_argument("-u", "--uncompressed", action="store_true",
                    help="Write all components uncompressed (storage mode 0x00)")

args = parser.parse_args()

# output file handling

if not args.output:
    args.output = Path(args.input.stem)

if args.output.exists() and not args.force:
    print(f"Output directory {args.output} exists, aborting.")
    exit()
else:
    print(f"Using output directory {args.output}")
    if not args.output.exists():
        args.output.mkdir()


### VRT processing

print("Processing VRT...")

corpus = [] # list of lists for utf-8 encoded strings indexed by [attr_i][cpos]
stack = [] # parsing stack for s attrs, list of openend tags (startpos, tagname, attrs)
spans = dict() # keys are the different s attrs, values list of spans (startpos, endpos)
span_attrs = dict() # same as above, but values are the associated attributes for each span 
cpos = 0
pcount = 0

# XML parser to parse s attrs
parser_state = (False, "", None) # (is_closing_tag, tagname, attributes)

def start_element(name, attrs):
    global parser_state
    parser_state = (False, name, attrs)

def end_element(name):
    global parser_state
    parser_state = (True, name, None)

parser = expat.ParserCreate()
parser.Parse("<start>") # init with one global start tag to keep parser happy
parser.StartElementHandler = start_element
parser.EndElementHandler = end_element

with args.input.open() as f:
    # find number of p attrs
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                pcount = len(line.split())
                break
    
    print(f"\t found {pcount} p-attrs")
    corpus = [[] for _ in range(pcount)]
    f.seek(0) # reset file to beginning

    for line in f:
        # p attrs
        if not line.startswith("<"):
            if line.strip():
                pattrs = line.split()
                for i, attr in enumerate(pattrs):
                    corpus[i].append((attr).encode("utf-8"))
                cpos += 1

        # s attrs
        else:
            parser.Parse(line)
            is_closing_tag, tagname, attrs = parser_state

            if not is_closing_tag:
                stack.append((cpos, tagname, attrs))
            else:
                startpos, start_tagname, attrs = stack.pop()
                if tagname == start_tagname:
                    if tagname not in spans.keys():
                        spans[tagname] = []
                        span_attrs[tagname] = []
                    spans[tagname].append((startpos, cpos))
                    span_attrs[tagname].append(attrs)

print(f"\t found {len(spans.keys())} s-attrs: {tuple(spans.keys())}")

clen = cpos

# double check dimensions
assert all(len(p) == clen for p in corpus), "P attributes of supplied VRT don't have the same dimensions"

print(f"Input corpus has {clen} corpus positions")


### Datastore creation
print("Building Ziggurat datastore...")

# A datastore consists of container files, which all have a UUID v4.
# Container files can be layer files and variables assigned to them.
# A datastore is built up from the bottom beginning with a primary layer that
# provides a global index of corpus positions (cpos), its variables, and additional
# layers that can reference layers below them.
# All these containers are linked via UUIDs.

# partition vector:
# no partition = 1 partition spanning the entire corpus
# with boundaries (0, clen)
partitions = [0, clen]


## Datastore Objects

datastore = dict()

# Primary Layer with corpus dimensions
datastore["primary_layer"] = PrimaryLayer(clen, partitions)

# Plain String Variable for tokens
datastore["p_token"] = PlainStringVariable(datastore["primary_layer"], corpus[0], compressed = not args.uncompressed)

# Indexed String Variable for POS tags
datastore["p_pos"] = IndexedStringVariable(datastore["primary_layer"], corpus[1], compressed= not args.uncompressed)

# Segmentation Layers for s attributes
for attr in spans.keys():
    slen = len(spans[attr])
    datastore["s_" + attr] = SegmentationLayer(slen, (0, slen), spans[attr])

for name, obj in datastore.items():
    ztype = obj.__class__.__name__
    ext = ".zigl" if isinstance(obj, Layer) else ".zigv"
    p = args.output / (name + ext)

    print(f"Writing {ztype} '{name}' to file {p}")
    with p.open(mode = "wb") as f:
        obj.write(f)
