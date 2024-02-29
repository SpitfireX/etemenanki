use std::{
    collections::HashMap, fs::File, io::{BufWriter, Seek, SeekFrom, Write}, mem, ops, slice, str::pattern::{Pattern, ReverseSearcher}
};

use memmap2::MmapOptions;
use regex::Regex;

use crate::container::BomEntry;

use super::{CachedVector, FnvHash, Index, InvertedIndex, Vector};

#[derive(Debug, Clone, Copy)]
pub struct StringVector<'map> {
    length: usize,
    offsets: &'map [i64],
    data: &'map [u8],
}

impl<'map> StringVector<'map> {
    pub fn all_matching_regex(&self, regex: &str) -> Option<MatchIterator<'map, impl Iterator<Item = usize> + '_>> {
        Regex::new(regex).ok()
            .map(|regex| {
                let iter = self.iter().enumerate()
                    .filter(move |(_, s)| regex.is_match(s))
                    .map(|(i, _)| i);

                MatchIterator {
                    strvec: *self,
                    inner: iter,
                }
            })
    }

    pub fn all_containing<'a, P>(&'a self, pattern: P) -> MatchIterator<'map, impl Iterator<Item = usize> + 'a>
    where
        P: Pattern<'a> + Copy + 'a,
        <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
    {
        let iter = self.iter().enumerate()
            .filter(move |(_, s)| pattern.is_contained_in(*s))
            .map(|(i, _)| i);

        MatchIterator {
            strvec: *self,
            inner: iter,
        }
    }

    pub fn all_ending_with<'a: 'map, P>(&'a self, pattern: P) -> MatchIterator<'map, impl Iterator<Item = usize> + 'a>
    where
    P: Pattern<'a> + Copy + 'a,
    <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
    {
        let iter = self.iter().enumerate()
            .filter(move |(_, s)| pattern.is_suffix_of(*s))
            .map(|(i, _)| i);
        
        MatchIterator {
            strvec: *self,
            inner: iter,
        }
    }

    pub fn all_starting_with<'a: 'map, P>(&'a self, pattern: P) -> MatchIterator<'map, impl Iterator<Item = usize> + 'a>
    where
    P: Pattern<'a> + Copy + 'a,
    <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
    {
        let iter = self.iter().enumerate()
            .filter(move |(_, s)| pattern.is_prefix_of(*s))
            .map(|(i, _)| i);

        MatchIterator {
            strvec: *self,
            inner: iter,
        }
    }

    pub fn from_parts(n: usize, offsets: &'map [i64], data: &'map [u8]) -> Self {
        assert!(n + 1 == offsets.len());
        Self {
            length: n,
            offsets,
            data,
        }
    }

    pub fn get(&self, index: usize) -> Option<&'map str> {
        if index < self.len() {
            Some(&self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &'map str {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        unsafe { std::str::from_utf8_unchecked(&self.data[start..end - 1]) }
    }

    pub fn get_all<'a: 'map, I>(&'a self, indices: I) -> impl Iterator<Item = &'map str>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        indices.into_iter().map(|x| &self[*x])
    }

    pub fn iter(&self) -> StringVectorIterator {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub unsafe fn encode_to_container_file<S, I>(strings: I, n: usize, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64)
    where
        S: AsRef<str>,
        I: Iterator<Item=S>
    {
        file.seek(SeekFrom::Start(start_offset)).unwrap();
        let len_offsets = (n + 1) * mem::size_of::<i64>();

        file.set_len(start_offset + len_offsets as u64).unwrap();
        let mut mmap = unsafe { MmapOptions::new().offset(start_offset).len(len_offsets).map_mut(&*file).unwrap()};
        let offsets = unsafe { slice::from_raw_parts_mut(mmap.as_mut_ptr() as *mut usize, n + 1) };

        file.seek(SeekFrom::Start(start_offset + len_offsets as u64)).unwrap();
        let mut writer = BufWriter::new(file);

        let mut count = 0;
        let mut soffset = 0;

        for s in strings.take(n) {
            offsets[count] = soffset;

            let bytes = s.as_ref().as_bytes();
            writer.write_all(bytes).unwrap();
            writer.write_all(&0u8.to_le_bytes()).unwrap(); // null terminator

            soffset += bytes.len()+1;
            count += 1;
        }
        offsets[count] = soffset;
        writer.flush().unwrap();

        assert!(n == count, "Number of written strings differs from n");

        bom_entry.size = (len_offsets + soffset) as i64;
        bom_entry.param1 = count as i64;
        bom_entry.param2 = 0;
    }
}

impl<'map> ops::Index<usize> for StringVector<'map> {
    type Output = str;

    fn index(&self, index: usize) -> &'map Self::Output {
        &self.get_unchecked(index)
    }
}

pub struct StringVectorIterator<'map> {
    vec: StringVector<'map>,
    index: usize,
}

impl<'map> Iterator for StringVectorIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        match self.vec.get(self.index) {
            Some(str) => {
                self.index += 1;
                Some(str)
            }
            None => None,
        }
    }
}

impl<'a, 'map> IntoIterator for &'a StringVector<'map> {
    type Item = &'map str;
    type IntoIter = StringVectorIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        StringVectorIterator {
            vec: *self,
            index: 0,
        }
    }
}

pub struct MatchIterator<'map, I>
where
    I: Iterator<Item = usize>
{
    strvec: StringVector<'map>,
    inner: I,
}

impl<'map, I> MatchIterator<'map, I>
where
    I: Iterator<Item = usize>
{
    pub fn as_strs(self) -> impl Iterator<Item = &'map str> {
        let MatchIterator{strvec, inner} = self;
        inner.map(move |i| strvec.get_unchecked(i))
    }
}

impl<'map, I> Iterator for MatchIterator<'map, I>
where
    I: Iterator<Item = usize>
{
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct LexiconBuilder {
    types: Vec<(String, usize)>,
    type_idx: HashMap<i64, usize>,
    id_stream_data: Vec<u8>,
    id_stream_sync: Vec<i64>,
    length: usize,
}

impl LexiconBuilder {
    pub fn new() -> Self {
        Self {
            types: Vec::new(),
            type_idx: HashMap::new(),
            id_stream_data: Vec::new(),
            id_stream_sync: Vec::new(),
            length: 0,
        }
    }

    fn encode_block(&mut self, block: &[i64]) {
        let mut buffer = [0; 16 * 9];
        let len = ziggurat_varint::encode_block_into(block, &mut buffer);

        self.id_stream_data.extend_from_slice(&buffer[..len]);

        if let Some(offset) = self.id_stream_sync.last() {
            self.id_stream_sync.push(offset + len as i64);
        } else {
            self.id_stream_sync.push(0);
            self.id_stream_sync.push(len as i64);
        }
    }

    fn get_id_or_add(&mut self, token: &str) -> usize {
        let hash = token.fnv_hash();

        match self.type_idx.entry(hash) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let id = *entry.get();

                // increase count
                self.types[id].1 += 1;

                id
            }

            std::collections::hash_map::Entry::Vacant(entry) => {
                let id = self.types.len();

                // insert element
                entry.insert(id);
                self.types.push((token.into(), 1));

                id
            }
        }
    }

    pub fn add_strings<S, I>(&mut self, mut strings: I)
    where
        S: Into<String> + AsRef<str>,
        I: Iterator<Item = S>,
    {
        // preprocess the first SCAN entries to build an optimized lexicon
        const SCAN: usize = 1_000_000;
        let mut id_stream = Vec::with_capacity(SCAN);
        for s in strings.by_ref().take(SCAN) {
            id_stream.push(self.get_id_or_add(s.as_ref()));
        }

        // sort lexicon
        self.types.sort_unstable_by_key(|(_, count)| *count);
        self.types.reverse();

        // lookup table
        // from old id to new id
        let mut lut = vec![0; self.types.len()];
        for ni in 0..self.types.len() {
            let hash = &self.types[ni].0.fnv_hash();
            let oi = self.type_idx[hash];
            lut[oi] = ni;
            self.type_idx.insert(*hash, ni);
        }

        // transform id_stream from old to new ids
        for i in 0..id_stream.len() {
            id_stream[i] = lut[id_stream[i]];
        }

        let mut bufi = 0;
        let mut idbuf = [0i64; 16];

        // compress id_stream
        for (i, id) in id_stream.iter().enumerate() {
            self.length += 1;
            bufi = i % 16;
            idbuf[bufi] = *id as i64;
            if bufi == 15 {
                self.encode_block(&idbuf);
                bufi = 0;
            }
        }

        // encode the remainder (if any)
        for s in strings {
            let id = self.get_id_or_add(s.as_ref());

            // the id stream gets collected into compressed Vector blocks
            if bufi < idbuf.len() {
                idbuf[bufi] = id as i64;
                bufi += 1;
            } else {
                // spill buffer
                self.encode_block(&idbuf);

                idbuf[0] = id as i64;
                bufi = 1;
            }

            self.length += 1;
        }

        // finish last id_stream block
        for i in bufi+1..idbuf.len() {
            idbuf[i] = -1;
        }
        self.encode_block(&idbuf);
    }

    pub fn from_strings<S, I>(strings: I) -> Self 
    where
        S: Into<String> + AsRef<str>,
        I: Iterator<Item = S>,
    {
        let mut lex = Self::new();
        lex.add_strings(strings);
        lex
    }

    pub fn stats(&self) {
        println!("total ids: {}", self.length);
        println!("types: {:?}", self.types);
        println!("types index: {:?}", self.type_idx);
    }

    pub fn tokens(&self) -> usize {
        self.length
    }

    pub fn types(&self) -> usize {
        self.types.len()
    }

    pub fn get_type(&self, id: usize) -> &str {
        &self.types[id].0
    }

    pub fn get_id_stream(&self) -> Vector<'_> {
        Vector::Compressed { length: self.length, width: 1, sync: &self.id_stream_sync, data: &self.id_stream_data }
    }

    pub unsafe fn write_lexicon(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) {
        let strings = self.types.iter().map(|(s, _)| s);
        StringVector::encode_to_container_file(strings, self.types(), file, bom_entry, start_offset)
    }

    pub unsafe fn write_index(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) {
        let mut pairs: Vec<_> = self.type_idx.iter().map(|(k, v)| (*k, *v as i64)).collect();
        pairs.sort_unstable_by_key(|(k, _)| *k);
        
        Index::encode_uncompressed_to_container_file(pairs.iter().copied(), self.types(), file, bom_entry, start_offset);
    }

    pub unsafe fn write_id_stream(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64, compressed: bool) {
        if compressed {
            file.seek(SeekFrom::Start(start_offset)).unwrap();

            let m = (self.length-1) / 16 + 1;
            assert!(self.id_stream_sync.len() == m+1, "somehow encoded too many blocks?");
            let sync = slice::from_raw_parts(self.id_stream_sync.as_ptr() as *const u8, mem::size_of::<i64>() * m);
            file.write_all(sync).unwrap();
            bom_entry.size = sync.len() as i64;

            file.write_all(&self.id_stream_data).unwrap();
            bom_entry.size += self.id_stream_data.len() as i64;

            file.flush().unwrap();

            bom_entry.param1 = self.tokens() as i64;
            bom_entry.param2 = 1;
        } else {
            // this is fucking silly
            let cvec = CachedVector::<1>::new(self.get_id_stream()).unwrap();
            Vector::encode_uncompressed_to_container_file(cvec.column_iter(0), cvec.len(), cvec.width(), file, bom_entry, start_offset);
        }
    }

    pub fn write_inverted_index(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) {
        let cvec = CachedVector::<1>::new(self.get_id_stream()).unwrap();
        InvertedIndex::encode_to_container_file(self.types(), cvec.column_iter(0), self.tokens(), file, bom_entry, start_offset);
    }
}

#[cfg(test)]
mod tests {
    fn startswith() {
        
    }
}
