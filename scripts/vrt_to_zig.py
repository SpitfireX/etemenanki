#!/usr/bin/env python3

import argparse
import xml.parsers.expat as expat
import gzip
import tempfile

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
parser.add_argument("-p", action="append", metavar="p_attribute_name", default=[],
                    help="""Declares and names a p-attribute. Order of declaration must correspond to order of columns in input.
                    P-attributes are encoded as variables on the primary layer of the corpus.
                    Variable type can be specified with a colon after the name, i.e. 'pos:indexed'.
                    Valid variable types are: indexed, plain, int, delta, set, skip.
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

args = parser.parse_args()

p_attrs = []
for p in args.p:
    p = p.split(":", 1)
    if len(p) == 1:
        name, type = p[0], "indexed"
    else:
        name, type = p
    assert type in ("indexed", "plain", "int", "delta", "set", "skip"), f"Invalid variable type '{type}' for p-attribute '{name}'"
    # p-attr tokens get saved to a temporary file to avoid loading them into RAM
    temp = tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", prefix=name+"_", suffix=".zigtmp")
    p_attrs.append((name, type, temp))


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

with gzip.open(args.input, mode = "rt") if args.input.suffix == ".gz" else args.input.open() as f:
    # find number of p attrs
    for line in f:
        if not line.startswith("<"):
            if line.strip():
                pcount = len(line.split())
                break
    
    print(f"\t found {pcount} p-attrs in input")
    assert len(p_attrs) <= pcount, "Not enough columns for specified p-attrs in input"
    f.seek(0) # reset file to beginning

    for line in f:
        # p attrs
        if not line.startswith("<"):
            if line.strip():
                cols = line.strip().split("\t", maxsplit = pcount-1)
                for i, token in enumerate(cols[:len(p_attrs)]):
                    p_attrs[i][2].write(token)
                    p_attrs[i][2].write("\n")
                cpos += 1

        # s attrs
        else:
            parser.Parse(line)
            is_closing_tag, tagname, attrs = parser_state

            if not is_closing_tag:
                stack.append((cpos, tagname, attrs))
            else:
                if len(stack) > 0 and stack[-1][1] == tagname:
                    startpos, start_tagname, attrs = stack.pop()
                    if tagname not in spans.keys():
                        spans[tagname] = []
                        span_attrs[tagname] = []
                    spans[tagname].append((startpos, cpos))
                    span_attrs[tagname].append(attrs)

print(f"\t found {len(spans.keys())} s-attrs: {tuple(spans.keys())}")

print("Encoding the following attributes:")
for name, type, _ in p_attrs:
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

clen = cpos

print(f"Input corpus has {clen} corpus positions")


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

def parse_int(str):
    try:
        return int(str)
    except Exception as e:
        if args.int_default:
            return args.int_default
        else:
            raise e


## Primary Layer with corpus dimensions
primary_layer = PrimaryLayer(clen, comment = f"{args.input.name}")
write_datastore_object(primary_layer, "primary")


## Primary Layer Variables for p attributes

for i, (name, type, temp) in enumerate(p_attrs):
    temp.file.seek(0)
    c = f"p-attr {name}"

    try:
        if type == "indexed":
            variable = FileIndexedStringVariable(primary_layer, temp, compressed = not args.uncompressed, comment = c)
        elif type == "plain":
            variable = PlainStringVariable(primary_layer, (line.strip() for line in temp), compressed = not args.uncompressed, comment = c)
        elif type == "int":
            variable = IntegerVariable(primary_layer, [parse_int(s) for s in temp], compressed = not args.uncompressed, comment = c)
        elif type == "delta":
            variable = IntegerVariable(primary_layer, [parse_int(s) for s in temp], compressed = not args.uncompressed, delta= True, comment = c)
        elif type == "set":
            variable = SetVariable(primary_layer, [parse_set(s) for s in temp], comment = c)
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
    slen = len(spans[attr])
    layer = SegmentationLayer(primary_layer, slen, spans[attr], compressed = not args.uncompressed, comment = f"s-attr {attr}")

    s_attr_layers[attr] = layer
    write_datastore_object(layer, f"{attr}/{attr}")


## Variables for s attribute annotations

for attr, annos in s_annos.items():
    base_layer = s_attr_layers[attr]

    for anno, type in annos:

        data = [attrs[anno] for attrs in span_attrs[attr]]
        assert len(data) == base_layer.n, f"Inconsistend number of annotations for annotation '{anno}' for s attribute '{attr}'"

        c = f"s-attr {attr}_{anno}"

        try:
            if type == "indexed":
                variable = IndexedStringVariable(base_layer, [s.encode("utf-8") for s in data], compressed = not args.uncompressed, comment = c)
            elif type == "plain":
                variable = PlainStringVariable(base_layer, data, compressed = not args.uncompressed, comment = c)
            elif type == "int":
                variable = IntegerVariable(base_layer, [parse_int(s) for s in data], compressed = not args.uncompressed, comment = c)
            elif type == "delta":
                variable = IntegerVariable(base_layer, [parse_int(s) for s in data], compressed = not args.uncompressed, delta=True, comment = c)
            elif type == "set":
                variable = SetVariable(base_layer, [parse_set(s) for s in data], comment = c)
            else:
                print(f"Invalid type '{type}' for annotation '{anno}' of s attribute '{attr}'")
                continue
        except Exception as e:
            print(f"Error while encoding annotation {anno} for s attribute '{attr}': {e}")
            exit()

        write_datastore_object(variable, f"{attr}/{anno}")
