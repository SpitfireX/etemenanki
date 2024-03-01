use core::panic;
use std::collections::HashMap;

use crate::components::FnvHash;

#[derive(Debug, Clone, Copy)]
pub struct Set<'map> {
    length: usize,
    width: usize,
    sync: &'map [i64],
    data: &'map [u8],
}

impl<'map> Set<'map> {
    pub fn from_parts(n: usize, p: usize, sync: &'map [i64], data: &'map [u8]) -> Self {
        assert!(p == 1, "Set with p > 1 not yet supported");
        Self {
            length: n,
            width: p,
            sync,
            data,
        }
    }

    pub fn get(&self, index: usize) -> Option<Vec<i64>> {
        if index < self.len() {
            Some(self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> Vec<i64> {
        let bi = index/16;
        if bi > self.sync.len() {
            panic!("block index out of range");
        }

        let mut offset = (self.sync[bi] as usize) - (self.sync.len() * 8);
        let ii = index % 16;

        let (item_offsets, readlen) = ziggurat_varint::decode_delta_array::<16>(&self.data[offset..]);
        offset += readlen;

        let (item_lens, readlen) = ziggurat_varint::decode_array::<16>(&self.data[offset..]);
        offset += readlen;

        let item_offset = offset + item_offsets[ii] as usize;
        let item_len = item_lens[ii] as usize;
        let (set, _) = ziggurat_varint::decode_fixed_block(&self.data[item_offset..], item_len);
        set
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn width(&self) -> usize {
        self.width
    }
}


pub struct SetBuilder {
    types: Vec<(String, usize)>,
    type_idx: HashMap<i64, usize>,
    id_stream_data: Vec<u8>,
    id_stream_sync: Vec<i64>,
    length: usize,
}

impl SetBuilder {
    pub fn new() -> Self {
        Self {
            types: Vec::new(),
            type_idx: HashMap::new(),
            id_stream_data: Vec::new(),
            id_stream_sync: Vec::new(),
            length: 0,
        }
    }

    fn encode_block(&mut self, block: &[Vec<i64>]) {
        let mut buffer = Vec::new();
        let mut len = 0;

        todo!();
        
        for set in block.iter() {
            buffer.resize(set.len() * 9, 0);
            let slen = ziggurat_varint::encode_block_into(set, &mut buffer);
            self.id_stream_data.extend_from_slice(&buffer[..slen]);
            len += slen;
        }

        if let Some(offset) = self.id_stream_sync.last() {
            self.id_stream_sync.push(offset + len as i64);
        } else {
            self.id_stream_sync.push(0);
            self.id_stream_sync.push(len as i64);
        }
    }

    fn get_id_or_add(&mut self, token: &str) -> i64 {
        let hash = token.fnv_hash();

        match self.type_idx.entry(hash) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let id = *entry.get();

                // increase count
                self.types[id].1 += 1;

                id as i64
            }

            std::collections::hash_map::Entry::Vacant(entry) => {
                let id = self.types.len();

                // insert element
                entry.insert(id);
                self.types.push((token.into(), 1));

                id as i64
            }
        }
    }

    pub fn add_sets<S, V, I>(&mut self, mut sets: I)
    where
        S: Into<String> + AsRef<str>,
        V: AsRef<[S]>,
        I: Iterator<Item = V>,
    {
        // preprocess the first SCAN entries to build an optimized lexicon
        const SCAN: usize = 1_000_000;
        let mut set_stream: Vec<Vec<i64>> = Vec::with_capacity(SCAN);
        for set in sets.by_ref().take(SCAN) {
            let mut temp: Vec<i64> = Vec::new();
            for s in set.as_ref() {
                temp.push(self.get_id_or_add(s.as_ref()));
            }
            set_stream.push(temp);
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

        // transform set_stream from old to new ids
        for set in set_stream.iter_mut() {
            for i in 0..set.len() {
                set[i] = lut[set[i as usize] as usize] as i64;
            }
        }

        let mut bufi = 0;
        let mut setbuf = vec![Vec::new(); 16];

        // compress set_stream
        for (i, set) in set_stream.into_iter().enumerate() {
            self.length += 1;
            bufi = i % 16;
            setbuf[bufi] = set;
            if bufi == 15 {
                self.encode_block(&setbuf);
                bufi = 0;
            }
        }

        // encode the remainder (if any)
        for set in sets {
            // the set stream gets collected into compressed Set blocks
            if bufi < setbuf.len() {
                setbuf[bufi].clear();
                for s in set.as_ref() {
                    let id = self.get_id_or_add(s.as_ref());
                    setbuf[bufi].push(id);
                }
                bufi += 1;
            } else {
                // spill buffer
                self.encode_block(&setbuf);

                setbuf[0].clear();
                for s in set.as_ref() {
                    let id = self.get_id_or_add(s.as_ref());
                    setbuf[0].push(id);
                }
                bufi = 1;
            }

            self.length += 1;
        }

        // finish last id_stream block
        for i in bufi+1..setbuf.len() {
            setbuf[i].clear();
        }
        self.encode_block(&setbuf[..]);
    }

    pub fn from_sets<S, V, I>(sets: I) -> Self 
    where
    S: Into<String> + AsRef<str>,
    V: AsRef<[S]>,
    I: Iterator<Item = V>,
    {
        let mut lex = Self::new();
        lex.add_sets(sets);
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

    // pub fn get_id_stream(&self) -> Vector<'_> {
    //     Vector::Compressed { length: self.length, width: 1, sync: &self.id_stream_sync, data: &self.id_stream_data }
    // }

    // pub unsafe fn write_lexicon(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) {
    //     let strings = self.types.iter().map(|(s, _)| s);
    //     StringVector::encode_to_container_file(strings, self.types(), file, bom_entry, start_offset)
    // }

    // pub unsafe fn write_index(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) {
    //     let mut pairs: Vec<_> = self.type_idx.iter().map(|(k, v)| (*k, *v as i64)).collect();
    //     pairs.sort_unstable_by_key(|(k, _)| *k);
        
    //     Index::encode_uncompressed_to_container_file(pairs.iter().copied(), self.types(), file, bom_entry, start_offset);
    // }

    // pub unsafe fn write_id_stream(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64, compressed: bool) {
    //     if compressed {
    //         file.seek(SeekFrom::Start(start_offset)).unwrap();

    //         let m = (self.length-1) / 16 + 1;
    //         assert!(self.id_stream_sync.len() == m+1, "somehow encoded too many blocks?");
    //         let sync = slice::from_raw_parts(self.id_stream_sync.as_ptr() as *const u8, mem::size_of::<i64>() * m);
    //         file.write_all(sync).unwrap();
    //         bom_entry.size = sync.len() as i64;

    //         file.write_all(&self.id_stream_data).unwrap();
    //         bom_entry.size += self.id_stream_data.len() as i64;

    //         file.flush().unwrap();

    //         bom_entry.param1 = self.tokens() as i64;
    //         bom_entry.param2 = 1;
    //     } else {
    //         // this is fucking silly
    //         let cvec = CachedVector::<1>::new(self.get_id_stream()).unwrap();
    //         Vector::encode_uncompressed_to_container_file(cvec.column_iter(0), cvec.len(), cvec.width(), file, bom_entry, start_offset);
    //     }
    // }

    // pub fn write_inverted_index(&self, file: &mut File, bom_entry: &mut BomEntry, start_offset: u64) {
    //     let cvec = CachedVector::<1>::new(self.get_id_stream()).unwrap();
    //     InvertedIndex::encode_to_container_file(self.types(), cvec.column_iter(0), self.tokens(), file, bom_entry, start_offset);
    // }
}