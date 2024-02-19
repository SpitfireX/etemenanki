#![feature(test)]

extern crate test;

use std::{fs::File, io::{BufRead, BufReader, Read, Result as IoResult}, str::FromStr};
use etemenanki::variables::IntegerVariable;
use flate2::read::GzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;

use pyo3::prelude::*;
use uuid::Uuid;

#[pymodule]
#[pyo3(name="_rustypy")]
fn module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_int_from_p, m)?)?;
    m.add_class::<IntVariableCore>()?;
    Ok(())
}

#[pyclass]
struct IntVariableCore {
    length: usize,
}

#[pymethods]
impl IntVariableCore {
    #[new]
    fn new(_filename: &str, length: usize) -> Self {
        Self {
            length
        }
    }

    fn __len__(&self) -> usize {
        self.length
    }
}

#[pyfunction]
fn encode_int_from_p(input: &str, column: usize, length: usize, default: i64, base: &str, compressed: bool, delta: bool, comment: &str, output: &str) {
    let reader = open_file(input).unwrap();
    let values = PIntIter {
        reader,
        column,
        default,
    };

    let base_uuid = Uuid::from_str(base).unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(output)
        .unwrap();
    IntegerVariable::encode_to_file(file, values, length, "bla".to_owned(), base_uuid, compressed, delta, comment);
}

struct PIntIter<R: Read> {
    reader: VrtReader<R>,
    column: usize,
    default: i64,
}

impl<R: Read> Iterator for PIntIter<R> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next_p(self.column).map(|(_, str)| str.parse().unwrap_or(self.default))
    }
}

enum LineEvent {
    Text,
    Tag,
}

pub struct VrtReader<R: Read> {
    reader: BufReader<R>,
    cpos: usize,
    last_line: String,
}

impl<R: Read> VrtReader<R> {
    pub fn new(readable: R) -> Self {
        Self { 
            reader: BufReader::new(readable),
            cpos: 0,
            last_line: String::new(),
        }
    }

    fn read_next(&mut self) -> Option<LineEvent> {
        self.last_line.clear();
        match self.reader.read_line(&mut self.last_line) {
            Ok(0) => None,

            Ok(_) => {
                if self.last_line.trim_start().starts_with('<') {
                    Some(LineEvent::Tag)
                } else {
                    self.cpos += 1;
                    Some(LineEvent::Text)
                }
            }

            Err(_) => None,
        }
    }

    pub fn next_p(&mut self, column: usize) -> Option<(usize, &str)> {
        while let Some(event) = self.read_next() {
            match event {
                LineEvent::Text => {
                    return self.last_line.trim()
                        .split('\t')
                        .nth(column)
                        .map(| token | return (self.cpos, token))
                }

                LineEvent::Tag => continue,
            }
        }
        None
    }

    pub fn next_s(&mut self, _tag: &str) -> Option<(usize, usize)> {
        todo!()
    }
}

pub fn open_file(filename: &str) -> IoResult<VrtReader<Box<dyn Read>>> {
    let file = File::open(filename)?;
    if filename.ends_with("gz") {
        Ok(VrtReader::new(Box::new(GzDecoder::new(file))))
    } else {
        Ok(VrtReader::new(Box::new(file)))
    }
}

pub fn open_file2(filename: &str) -> IoResult<VrtReader2<Box<dyn Read>>> {
    let file = File::open(filename)?;
    if filename.ends_with("gz") {
        Ok(VrtReader2::new(Box::new(GzDecoder::new(file))))
    } else {
        Ok(VrtReader2::new(Box::new(file)))
    }
}

pub struct VrtReader2<R: Read> {
    xml: Reader<BufReader<R>>,
    buffer: Vec<u8>,
    cpos: usize,
    lines: Option<Vec<String>>,
    lpos: usize,
}

impl<'a, R: Read> VrtReader2<R> {
    pub fn new(readable: R) -> Self {
        let bufreader = BufReader::new(readable);
        let mut reader = Reader::from_reader(bufreader);
        reader.trim_text(true);

        Self {
            xml: reader,
            buffer: Vec::new(),
            cpos: 0,
            lines: None,
            lpos: 0,
        }
    }

    fn read_next(&mut self) -> Option<Event> {
        self.buffer.clear();
        self.xml.read_event_into(&mut self.buffer).ok()
    }

    pub fn next_p(&mut self, column: usize) -> Option<(usize, &str)> {
        if let Some(lines) = &self.lines {
            if self.lpos >= lines.len(){
                self.lpos = 0;
                self.lines = None;
            }
        }

        if self.lines.is_none() {
            while let Some(event) = self.read_next() {
                match event {
                    Event::Text(t) => {
                        let lines: Result<Vec<_>, _> = t.lines().collect();
                        self.lines = lines.ok();
                        break;
                    }
    
                    Event::Eof => return None,
    
                    _ => continue,
                }
            }
        }

        if let Some(lines) = &self.lines {
            let token = lines[self.lpos].split('\t').nth(column)?;
            let value = (self.cpos, token);
            self.cpos += 1;
            self.lpos += 1;
            return Some(value);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use test::{Bencher, black_box};
    use crate::open_file;
    use crate::open_file2;

    #[test]
    fn it_works() {
        let mut file = open_file("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        file.read_next();
    }

    #[test]
    fn read_events() {
        let mut file = open_file("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        while let Some((cpos, text)) = file.next_p(0) {
            println!("{}: {}", cpos, text);
        }
    }

    #[bench]
    fn bench_read_p(b: &mut Bencher) {
        b.iter(||{
            let mut file = open_file("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
            while let Some(attr) = file.next_p(0) {
                black_box(attr);
            }
        });
    }

    #[test]
    fn read_events2() {
        let mut file = open_file2("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        while let Some((cpos, text)) = file.next_p(0) {
            println!("{}: {}", cpos, text);
        }
    }

    #[bench]
    fn bench_read_p2(b: &mut Bencher) {
        b.iter(||{
            let mut file = open_file2("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
            while let Some(attr) = file.next_p(0) {
                black_box(attr);
            }
        });
    }
}
