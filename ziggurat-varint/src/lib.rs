use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyfunction]
fn encode_varint(py: Python, x: i64) -> PyObject {
    let mut buffer = [0u8; 9];
    let len = x.encode_varint_into(&mut buffer);
    PyBytes::new(py, &buffer[..len]).into()
}

#[pyfunction]
fn encode_varint_unsigned(py: Python, x: u64) -> PyObject {
    let mut buffer = [0u8; 9];
    let len = (x as i64).encode_varint_into(&mut buffer);
    PyBytes::new(py, &buffer[..len]).into()
}

#[pyfunction]
fn encode_varint_block(py: Python, ints: Vec<i64>) -> PyObject {
    let mut buffer = vec![0u8; ints.len() * 9];
    let mut blen = 0;

    for int in ints {
        blen += int.encode_varint_into(&mut buffer[blen..(blen + 9)]);
    }

    PyBytes::new(py, &buffer[..blen]).into()
}

#[pyfunction]
fn encode_varint_block_unsigned(py: Python, ints: Vec<u64>) -> PyObject {
    let mut buffer = vec![0u8; ints.len() * 9];
    let mut blen = 0;

    for int in ints {
        blen += (int as i64).encode_varint_into(&mut buffer[blen..(blen + 9)]);
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

#[inline]
pub fn decode(bytes: &[u8]) -> (i64, usize) {
    let mut i = 0;
    let mut output = (bytes[i] & 0b00111111) as i64;
    let neg = (bytes[i] & 0b01000000) == 0b01000000;
    let mut cont = (bytes[i] & 0b10000000) == 0b10000000;

    while cont && i < 7 {
        i += 1;
        output <<= 7;
        output |= (bytes[i] & 0b01111111) as i64;
        cont = (bytes[i] & 0b10000000) == 0b10000000;
    }

    if cont {
        i += 1;
        output <<= 8;
        output |= (bytes[i] & 0xFF) as i64;
    }

    if neg {
        (!output, i + 1)
    } else {
        (output, i + 1)
    }
}

pub fn decode_block(bytes: &[u8]) -> (Vec<i64>, usize) {
    let mut offset = 0;
    let mut output = Vec::new();
    while offset < bytes.len() {
        let (i, len) = decode(&bytes[offset..]);
        output.push(i);
        offset += len;
    }
    (output, offset)
}

pub fn decode_array<const N: usize>(bytes: &[u8]) -> ([i64; N], usize) {
    let mut offset = 0;
    let mut output = [0; N];
    for i in 0..N {
        let (int, readlen) = decode(&bytes[offset..]);
        output[i] = int;
        offset += readlen;
    }
    (output, offset)
}

pub fn decode_delta_array<const N: usize>(bytes: &[u8]) -> ([i64; N], usize) {
    let mut offset = 0;
    let mut output = [0; N];

    if N > 0 {
        // first value
        let (int, readlen) = decode(&bytes[offset..]);
        output[0] = int;
        offset += readlen;

        // rest
        for i in 1..N {
            let (int, readlen) = decode(&bytes[offset..]);
            output[i] = output[i-1] + int;
            offset += readlen;
        }
    }

    (output, offset)
}

pub fn decode_fixed_block(bytes: &[u8], len: usize) -> (Vec<i64>, usize) {
    let mut offset = 0;
    let mut output = Vec::with_capacity(len);
    for _ in 0..len {
        let (int, readlen) = decode(&bytes[offset..]);
        output.push(int);
        offset += readlen;
    }
    (output, offset)
}

pub fn decode_fixed_delta_block(bytes: &[u8], len: usize) -> (Vec<i64>, usize) {
    let mut offset = 0;
    let mut output = Vec::with_capacity(len);

    if len > 0 {
        // first value
        let (int, readlen) = decode(&bytes[offset..]);
        output.push(int);
        offset += readlen;

        // rest
        for i in 1..len {
            let (int, readlen) = decode(&bytes[offset..]);
            output.push(output[i-1] + int);
            offset += readlen;
        }
    }

    (output, offset)
}

pub fn encode_block<I: EncodeVarint>(block: &[I]) -> Vec<u8> {
    let mut output = Vec::with_capacity(block.len() * 9);
    for i in block {
        let mut bytes = [0u8; 9];
        let len = i.encode_varint_into(&mut bytes[..]);
        output.extend_from_slice(&bytes[..len]);
    }
    output
}

pub fn encode_block_into<I: EncodeVarint>(block: &[I], buffer: &mut [u8]) -> usize {
    let mut offset = 0;
    for i in block {
        let len = i.encode_varint_into(&mut buffer[offset..]);
        offset += len;
    }
    offset
}

pub fn encode_delta_block<I: EncodeVarint + Copy + std::ops::Sub<I, Output = I>>(block: &[I]) -> Vec<u8> {
    let mut output = vec![0; block.len()*9];

    // first value raw
    let mut len = block[0].encode_varint_into(&mut output);

    //following values delta
    for i in 1..block.len() {
        let v = block[i] - block[i-1];
        len += v.encode_varint_into(&mut output[len..]);
    }

    output.truncate(len);
    output
}

pub fn encode_delta_block_into<I: EncodeVarint + Copy + std::ops::Sub<I, Output = I>>(block: &[I], buffer: &mut [u8]) -> usize {
    // first value raw
    let mut offset = block[0].encode_varint_into(buffer);

    // following values delta
    for i in 1..block.len() {
        let v = block[i] - block[i-1];
        offset += v.encode_varint_into(&mut buffer[offset..]);
    }

    offset
}

pub trait EncodeVarint {
    fn encode_varint(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; 9];
        let len = self.encode_varint_into(&mut buffer);
        buffer.truncate(len);
        buffer
    }

    fn encode_varint_into(&self, buffer: &mut [u8]) -> usize;
}

impl EncodeVarint for i64 {
    #[inline]
    fn encode_varint_into(&self, arr: &mut [u8]) -> usize {
        assert!(arr.len() >= 9, "passed slice too small to hold varint");
        let mut x = *self;

        let is_negative = x.is_negative();
        if is_negative {
            // invert if negative
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

        if n_bytes == 9 {
            // byte 9 with 8 bits
            arr[k] = (x & 0xff) as u8;
            x >>= 8;
            k -= 1;
        }

        while k > 0 && k < 9 {
            // byte 2..8 with 7 bits
            let mut byte = (x & 0x7f) as u8;
            x >>= 7;
            if k < n_bytes - 1 {
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

#[cfg(test)]
mod tests {

    use crate::EncodeVarint;

    #[test]
    fn boundaries() {
        let ints: Vec<i64> = vec![
            0,
            -64,
            63,
            -8192,
            8191,
            -1048576,
            1048575,
            -134217728,
            134217727,
            -17179869184,
            17179869183,
            -2199023255552,
            2199023255551,
            -281474976710656,
            281474976710655,
            -36028797018963968,
            36028797018963967,
            -9223372036854775808,
            9223372036854775807,
        ];

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
            let mut buffer = [0; 9];
            let len = i.encode_varint_into(&mut buffer);
            lens.push(len);
            encodings.push(buffer[..len].into());
        }

        assert_eq!(expected_lens, lens);
        assert_eq!(expected_encodings, encodings);
    }
}
