#![feature(test)]

extern crate test;

use std::{fs::File, io::{BufRead, BufReader, Read, Result as IoResult}};
use flate2::read::GzDecoder;
use quick_xml::events::{BytesText, Event};
use quick_xml::reader::Reader;

use pyo3::prelude::*;

#[pymodule]
#[pyo3(name="_rustypy")]
fn module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
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
    fn new(filename: &str, length: usize) -> Self {
        Self {
            length
        }
    }

    fn __len__(&self) -> usize {
        self.length
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

    pub fn next_s(&mut self, tag: &str) -> Option<(usize, usize)> {
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
    use std::fs::File;

    use flate2::read::GzDecoder;
    use test::{Bencher, black_box};
    use crate::open_file;
    use crate::open_file2;
    use crate::VrtReader;

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
