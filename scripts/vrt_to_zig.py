#!/usr/bin/env python3

import argparse
import xml.parsers.expat as expat
import gzip
import tempfile

from ziggypy.components import *
from ziggypy.layers import *
from ziggypy.variables import *
from ziggypy.util import PFileIter, SFileIter
from ziggypy._rustypy import vrt_stats

from os.path import realpath
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
parser.add_argument("-x", "--invalid-xml", action="store_true",
                    help="Fix invalid XML. Encloses whole corpus in an additional root element.")
parser.add_argument("-p", action="append", metavar="p_attribute_name", default=[],
                    help="""Declares and names a p-attribute. Order of declaration must correspond to order of columns in input.
                    P-attributes are encoded as variables on the primary layer of the corpus.
                    Variable type can be specified with a colon after the name, i.e. 'pos:indexed'.
                    Valid variable types are: indexed, plain, int, delta, set, ptr, skip.
                    The type "ptr" as of now only works for a singluar ptr attribute per encoding run and is intended to encode
                    universal dependencies style dependency relations. See "--ptr-base" below for more details. 
                    The type "skip" denotes that a column should be skipped and not encoded.""")
parser.add_argument("-s", action="append", metavar="s_attribute_name", default=[],
                    help="""Declares an s-attribute. The attribute name must correspond to the attribute's XML tag in the input.
                    S-attributes are encoded as segmentation layers and thus only store the start and end positions of the spans
                    enclosed by the XML tags.
                    e.g. '-s text'.
                    For encoding of the tag's attributes see '-a'.""")
parser.add_argument("-a", action="append", metavar="s_annotation_spec", default=[],
                    help="""Declares an annotation spec for an s-attribute annotation. In the input these annotations correspond
                    with the attributes of the s-attribute's XML tags. Annotations consist of three parts: The s-attribute's name,
                    the annotation's name, and a Ziggurat variable type. This takes the form 's_attr+name:type',
                    e.g. '-a text+url:plain'.
                    Valid variable types are: indexed, plain, int, delta, set.
                    """)
parser.add_argument("--int-default", type=int, metavar="int_default",
                    help="""The default value used when an invalid integer value is encountered while encoding an attribute.
                    If no default is given, the encoder will exit with an error (default behavior).""")
parser.add_argument("--ptr-base", type=str, metavar="ptr_base",
                    help="""This argument denotes the name of a p-attribute for use as a reference for pointer calculation in 
                    combination with the "ptr" attribute type. Said p-attribute needs to be specified with the "-p" flag and may
                    be of type "skip", but the actual data in the input file must be ints.
                    Assuming the arguments "-p index:skip -p head:ptr --ptr-base index"
                    pointers are calculated via the following formula: corpus_position + (head - index)""")

args = parser.parse_args()

p_attrs = []
for p in args.p:
    p = p.split(":", 1)
    if len(p) == 1:
        name, type = p[0], "indexed"
    else:
        name, type = p
    assert type in ("indexed", "plain", "int", "delta", "set", "ptr", "skip"), f"Invalid variable type '{type}' for p-attribute '{name}'"
    p_attrs.append((name, type))

# validation just for ptr variables
ptrcount = [p[1] for p in p_attrs].count("ptr")
assert  ptrcount <= 1, "maximum of one pointer variable can be encoded at the same time"
if ptrcount > 0:
    assert args.ptr_base, "--ptr-base must be specified for encoding pointer variables"
    try:
        next(n for (n, _) in p_attrs if n == args.ptr_base)
    except:
        raise ValueError(f"specified ptr-base '{args.ptr_base}' does not exist in specified p-attributes"
)


s_attrs = args.s

s_annos = dict()
for a in args.a:
    try:
        attr, anno = a.split("+")
        anno, type = anno.split(":")
        assert type in ("indexed", "plain", "int", "delta", "set"), f"Invalid variable type '{type}' for annotation {anno} for s-attribute '{attr}'"
        if attr not in s_annos.keys():
            s_annos[attr] = []
        s_annos[attr].append((anno, type))
    except:
        print(f"Invalid s-attribute annotation spec '{a}'")
        exit()


### VRT processing

print("Scanning VRT...")

def open_input():
    if args.input.suffix == ".gz":
        return gzip.open(args.input, mode = "rt")
    else:
        return args.input.open()

# scan file
clen, pcount, scounts = vrt_stats(realpath(args.input))

print(f"\t found {pcount} p-attrs in input")
print(f"\t found {len(scounts.keys())} s-attrs: {scounts}")

assert len(p_attrs) <= pcount, "Not enough columns for specified p-attrs in input"

assert all(s in scounts.keys() for s in s_attrs), "Specified s-attrs are not present in input file"
assert all(a in scounts.keys() for a in s_annos.keys()), "Specified s-attr annotations are not present in input file"

print("Encoding the following attributes:")
for name, type in p_attrs:
    if type == "ptr":
        print(f"\tp-attribute '{name}' of type '{type}' with base '{args.ptr_base}'")
    elif type != "skip":
        print(f"\tp-attribute '{name}' of type '{type}'")
for name in s_attrs:
    print(f"\ts-attribute '{name}'", end="")
    if name in s_annos.keys() and len(s_annos[name]) > 0:
        print(f"with annotation {'s' if len(s_annos[name]) > 1 else ''}")
    else:
        print()
    if name in s_annos.keys():
        for annotation, type in s_annos[name]:
            print(f"\t\t'{annotation}' of type '{type}'")

if not p_attrs and not s_attrs and not s_annos:
    print("\tNo attributes for encoding")
    print("Hint: you should probably specify some with -p, -s, or -a (see --help)")
    exit()

print(f"Input corpus has {clen} corpus positions")


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


### Datastore creation
print("Building Ziggurat datastore...")

# A datastore consists of container files, which all have a UUID v4.
# Container files can be layer files and variables assigned to them.
# A datastore is built up from the bottom beginning with a primary layer that
# provides a global index of corpus positions (cpos), its variables, and additional
# layers that can reference layers below them.
# All these containers are linked via UUIDs.


def write_datastore_object(obj, filename):
    ztype = obj.__class__.__name__
    ext = ".zigl" if isinstance(obj, Layer) else ".zigv"
    p = args.output / (filename + ext)
    p.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing {ztype} '{filename}' to file {p}")
    with p.open(mode = "wb") as f:
        obj.write(f)


def parse_set(str):
    return set(s.encode("utf-8") for s in str.strip().split("|") if s)


## Primary Layer with corpus dimensions
primary_layer = PrimaryLayer(clen, comment = f"{args.input.name}")
write_datastore_object(primary_layer, "primary")


## Primary Layer Variables for p attributes

for i, (name, type) in enumerate(p_attrs):
    c = f"p-attr {name}"

    with open_input() as f:
        fileiter = PFileIter(f, i)

        try:
            if type == "indexed":
                variable = FileIndexedStringVariable(primary_layer, fileiter, compressed = not args.uncompressed, comment = c)
            elif type == "plain":
                variable = PlainStringVariable(primary_layer, fileiter, compressed = not args.uncompressed, comment = c)
            elif type == "int":
                variable = RustyIntegerVariable(primary_layer, f, i, clen, compressed = not args.uncompressed, comment = c, default=args.int_default)
            elif type == "delta":
                variable = RustyIntegerVariable(primary_layer, f, i, clen, compressed = not args.uncompressed, comment = c, default=args.int_default, delta=True)
            elif type == "set":
                variable = FileSetVariable(primary_layer, fileiter, clen, parse_set, comment = c)
            elif type == "ptr":
                base_index = next(i for i, (n, _) in enumerate(p_attrs) if n == args.ptr_base)
                variable = RustyPointerVariable(primary_layer, f, base_index, i, clen, compressed = not args.uncompressed, comment = c)
            elif type == "skip":
                continue
            else:
                print(f"Invalid type '{type}' for p attribute '{name}'")
                continue
        except Exception as e:
            print(f"Error while encoding p attribute '{name}': {e}")
            exit()

    write_datastore_object(variable, name)


s_attr_layers = dict()

## Segmentation Layers for s attributes

for attr in s_attrs:

    layer = RustySegmentationLayer(primary_layer, open_input(), attr, scounts[attr], compressed = not args.uncompressed, comment = f"s-attr {attr}")

    s_attr_layers[attr] = layer
    write_datastore_object(layer, f"{attr}/{attr}")


## Variables for s attribute annotations

for attr, annos in s_annos.items():
    base_layer = s_attr_layers[attr]

    for anno, type in annos:
        with open_input() as f:
            fileiter = SFileIter(f, attr, fix=args.invalid_xml)
            data = [a[anno] for _, a in fileiter]

        assert len(data) == base_layer.n, f"Inconsistend number of annotations for annotation '{anno}' for s attribute '{attr}'"

        c = f"s-attr {attr}_{anno}"

        try:
            if type == "indexed":
                variable = IndexedStringVariable(base_layer, [s.encode("utf-8") for s in data], compressed = not args.uncompressed, comment = c)
            elif type == "plain":
                variable = PlainStringVariable(base_layer, data, compressed = not args.uncompressed, comment = c)
            elif type == "int":
                variable = RustyIntegerVariable(base_layer, f, (attr, anno), base_layer.n, compressed = not args.uncompressed, comment = c, default=args.int_default)
            elif type == "delta":
                variable = RustyIntegerVariable(base_layer, f, (attr, anno), base_layer.n, compressed = not args.uncompressed, comment = c, default=args.int_default, delta=True)
            elif type == "set":
                variable = SetVariable(base_layer, [parse_set(s) for s in data], comment = c)
            else:
                print(f"Invalid type '{type}' for annotation '{anno}' of s attribute '{attr}'")
                continue
        except Exception as e:
            print(f"Error while encoding annotation {anno} for s attribute '{attr}': {e}")
            exit()

        write_datastore_object(variable, f"{attr}/{anno}")
