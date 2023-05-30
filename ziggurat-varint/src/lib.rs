use std::convert::From;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn encode_varint(py: Python, x: i64) -> PyObject {
    let mut buffer = [0u8; 9];
    let v: VarInt = x.into();
    let len = v.encode(&mut buffer);
    PyBytes::new(py, &buffer[..len]).into()
}

#[pyfunction]
fn encode_varint_unsigned(py: Python, x: u64) -> PyObject {
    let mut buffer = [0u8; 9];
    let v: VarInt = (x as i64).into();
    let len = v.encode(&mut buffer);
    PyBytes::new(py, &buffer[..len]).into()
}

#[pyfunction]
fn encode_varint_block(py: Python, ints: Vec<i64>) -> PyObject {
    let mut buffer = vec![0u8; ints.len() * 9];
    let mut blen = 0;

    for int in ints.iter() {
        let v: VarInt = (*int).into();
        blen += v.encode(&mut buffer[blen..(blen+9)]);
    }

    PyBytes::new(py, &buffer[..blen]).into()
}

#[pyfunction]
fn encode_varint_block_unsigned(py: Python, ints: Vec<u64>) -> PyObject {
    let mut buffer = vec![0u8; ints.len() * 9];
    let mut blen = 0;

    for int in ints.iter() {
        let v: VarInt = (*int as i64).into();
        blen += v.encode(&mut buffer[blen..(blen+9)]);
    }

    PyBytes::new(py, &buffer[..blen]).into()
}

/// A Python module implemented in Rust.
#[pymodule]
fn ziggurat_varint(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_varint, m)?)?;
    m.add_function(wrap_pyfunction!(encode_varint_unsigned, m)?)?;
    m.add_function(wrap_pyfunction!(encode_varint_block, m)?)?;
    m.add_function(wrap_pyfunction!(encode_varint_block_unsigned, m)?)?;
    Ok(())
}

#[repr(transparent)]
pub struct VarInt(i64);

impl VarInt {

    #[inline]
    pub fn encode(&self, arr: &mut [u8]) -> usize {
        assert!(arr.len() >= 9, "passed slice too small to hold VarInt");
        
        let VarInt(mut x) = *self;

        let is_negative = x.is_negative();
        if is_negative { // invert if negative
            x = !x;
        }
        
        let mut mask = -1 << 6;
    
        let mut n_bytes = 1; // find number of bytes needed
        while (x & mask) != 0 {
            match n_bytes {
                8 => mask <<= 8,
                _ => mask <<= 7,
            }
            n_bytes += 1;
        }
    
        let mut k = n_bytes - 1;
        
        if n_bytes == 9 { // byte 9 with 8 bits
            arr[k] = (x & 0xff) as u8;
            x >>= 8;
            k -= 1;
        }
    
        while k > 0 && k < 9 { // byte 2..8 with 7 bits
            let mut byte = (x & 0x7f) as u8;
            x >>= 7;
            if k < n_bytes-1 { 
                byte |= 0x80;
            }
            arr[k] = byte;
            k -= 1;
        }
    
        let mut byte = (x & 0x3f) as u8; // byte 1 with 6 bits
        if n_bytes > 1 {
            byte |= 0x80; // set continuation bit
        }
        if is_negative {
            byte |= 0x40; // set sign bit
        }
        arr[0] = byte;
    
        n_bytes
    }
}

impl From<i64> for VarInt {

    #[inline]
    fn from(value: i64) -> VarInt {
        VarInt(value)
    }

}

#[cfg(test)]
mod tests {

    use crate::VarInt;

    #[test]
    fn into() {
        let a: i64 = 1337;
        let v: VarInt = a.into();
    }

    #[test]
    fn boundaries() {
        let ints: Vec<i64> = vec![0, -64, 63, -8192, 8191, -1048576, 1048575, -134217728, 134217727, -17179869184, 17179869183, -2199023255552, 2199023255551, -281474976710656, 281474976710655, -36028797018963968, 36028797018963967, -9223372036854775808, 9223372036854775807];

        let expected_lens = vec![1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9];

        let expected_encodings = vec![
            vec![0x00],
            vec![0x7F],
            vec![0x3F],
            vec![0xFF, 0x7F],
            vec![0xBF, 0x7F],
            vec![0xFF, 0xFF, 0x7F],
            vec![0xBF, 0xFF, 0x7F],
            vec![0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xBF, 0xFF, 0xFF, 0x7F],
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xBF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            vec![0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        ];

        let mut lens = Vec::new();
        let mut encodings: Vec<Vec<u8>> = Vec::new();

        for i in ints {
            let v: VarInt = i.into();
            let mut buffer = [0; 9];
            let len = v.encode(&mut buffer);
            lens.push(len);
            encodings.push(buffer[..len].into());
        }

        assert_eq!(expected_lens, lens);
        assert_eq!(expected_encodings, encodings);
    }
}
