use core::panic;

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

        let (item_lens, _) = ziggurat_varint::decode_array::<16>(&self.data[offset..]);
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
