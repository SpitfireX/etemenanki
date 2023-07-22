#[derive(Debug, Clone, Copy)]
pub struct InvertedIndex<'map> {
    types: usize,
    jtable_length: usize,
    typeinfo: &'map [(i64, i64)],
    data: &'map [u8],
}

impl<'map> InvertedIndex<'map> {
    pub fn from_parts(k: usize, p: usize, typeinfo: &'map [(i64, i64)], data: &'map [u8]) -> Self {
        Self {
            types: k,
            jtable_length: p,
            typeinfo,
            data,
        }
    }

    /// Returns the frequency of type `i`
    pub fn frequency(&self, i: usize) -> usize {
        self.typeinfo[i].0 as usize
    }

    /// Returns the number of types in this index
    pub fn n_types(&self) -> usize {
        self.types
    }

    /// Returns the start offset of the postings list for type `i`
    /// within the `data` component
    pub fn offset(&self, i: usize) -> usize {
        self.typeinfo[i].1 as usize - (self.n_types() * 16)
    }

    /// Returns an iterator over the postings for type `i`
    pub fn postings(&self, i: usize) -> PostingsIterator {
        let slice = if i < self.n_types() - 1 {
            &self.data[self.offset(i)..self.offset(i + 1)]
        } else {
            &self.data[self.offset(i)..]
        };

        let (value, readlen) = ziggurat_varint::decode(&slice);
        let slice = &slice[readlen..];

        PostingsIterator {
            data: slice,
            len: self.frequency(i),
            i: 0,
            offset: 0,
            value: 0,
            jtable_offset: value as usize,
        }
    }
}

pub struct PostingsIterator<'map> {
    data: &'map [u8],
    len: usize,
    i: usize,
    offset: usize,
    value: usize,
    jtable_offset: usize,
}

impl<'map> Iterator for PostingsIterator<'map> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.len {
            let (value, readlen) = ziggurat_varint::decode(&self.data[self.offset..]);
            self.i += 1;
            self.offset += readlen;
            self.value += value as usize;
            Some(self.value)
        } else {
            None
        }
    }
}
