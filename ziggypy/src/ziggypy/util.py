from itertools import islice
from typing import Iterable, Any
from xml.parsers import expat


def batched(iterable: Iterable[Any], n: int):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch


class PFileIter:
    """Resettable iterator that wraps around an open text file in VRT format.
    The iterator returns values of the p attribute in the file at `column` as
    strings."""

    def __init__(self, file, column, total_cols=-1):
        self.file = file
        self.column = column
        self.total_cols = total_cols
        self.lines = 0

    def __iter__(self):
        self.reset()
        return self

    def __next__(self):
        for line in self.file:
            self.lines += 1
            if not line.startswith("<"):
                if line.strip():
                    cols = line.strip().split("\t", maxsplit = self.total_cols-1)
                    if self.column < len(cols):
                        return cols[self.column]
                    else:
                        raise IndexError(f"not enough columns in line {self.lines}")
        raise StopIteration

    def reset(self):
        self.file.seek(0)

class SFileIter:
    """Resettable iterator that wraps around an open text file in VRT format.
    The iterator returns values for an s attribute in the file matching `tagname`
    as tuples of the following format: ((start_position, end_position), {attrs})"""

    def __init__(self, file, tagname, fix=False):
        self.file = file
        self.tagname = tagname
        self.fix = fix
        self.line = 0

    def reset(self):
        self.file.seek(0)

        self.stack = []
        self.parser_state = (False, "", None) # (is_closing_tag, tagname, attributes)

        def start_element(name, attrs):
            self.parser_state = (False, name, attrs)

        def end_element(name):
            self.parser_state = (True, name, None)

        parser = expat.ParserCreate()
        if self.fix:
            parser.Parse("<xml-fix-pseudo-start>") # init with one global start tag to keep parser happy
        parser.StartElementHandler = start_element
        parser.EndElementHandler = end_element

        self.parser = parser

    def __iter__(self):
        self.reset()
        return self
    
    def __next__(self):
        for line in self.file:
            if line.startswith("<"):
                self.parser.Parse(line)
                is_closing_tag, tagname, attrs = self.parser_state

                if tagname == self.tagname:
                    if not is_closing_tag:
                        self.stack.append((self.line, tagname, attrs))
                    else:
                        if len(self.stack) > 0 and self.stack[-1][1] == tagname:
                            startpos, start_tagname, attrs = self.stack.pop()
                            return ((startpos, self.line), attrs)
            else:
                self.line += 1

        raise StopIteration
