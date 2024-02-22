#![feature(test)]

extern crate test;

use std::{collections::{HashMap, VecDeque}, fs::File, io::{BufRead, BufReader, Read, Result as IoResult}, str::FromStr};
use etemenanki::{layers::SegmentationLayer, variables::{IndexedStringVariable, IntegerVariable, PointerVariable}};
use flate2::read::MultiGzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;

use pyo3::prelude::*;
use uuid::Uuid;

#[pymodule]
#[pyo3(name="_rustypy")]
fn module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_indexed_from_p, m)?)?;
    m.add_function(wrap_pyfunction!(encode_ptr_from_p, m)?)?;
    m.add_function(wrap_pyfunction!(encode_seg_from_s, m)?)?;
    m.add_function(wrap_pyfunction!(encode_int_from_a, m)?)?;
    m.add_function(wrap_pyfunction!(encode_int_from_p, m)?)?;
    m.add_function(wrap_pyfunction!(vrt_stats, m)?)?;
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
fn encode_indexed_from_p(input: &str, column: usize, length: usize, base: &str, compressed: bool, comment: &str, output: &str){
    let reader = open_reader(input).unwrap();
    let strings = reader.iter_p(column).map(|(_, s)| s);

    let base_uuid = Uuid::from_str(base).unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(output)
        .unwrap();

    IndexedStringVariable::encode_to_file(file, strings, length, "mar".to_owned(), base_uuid, compressed, comment);
}

#[pyfunction]
fn encode_int_from_p(input: &str, column: usize, length: usize, default: i64, base: &str, compressed: bool, delta: bool, comment: &str, output: &str) {
    let reader = open_reader(input).unwrap();
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

#[pyfunction]
fn encode_int_from_a(input: &str, tag: &str, attr: &str, length: usize, default: i64, base: &str, compressed: bool, delta: bool, comment: &str, output: &str) {
    let parser = open_parser(input).unwrap();
    let values = parser
        .a_iter(tag, attr)
        .map(|(_, _, str)| str.parse().unwrap_or(default));

    let base_uuid = Uuid::from_str(base).unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(output)
        .unwrap();

    IntegerVariable::encode_to_file(file, values, length, "bla".to_owned(), base_uuid, compressed, delta, comment);
}

#[pyfunction]
fn encode_seg_from_s(input: &str, s_tag: &str, length: usize, base: &str, compressed: bool, comment: &str, output: &str) -> (usize, String) {
    let parser = open_parser(input).unwrap();
    let values = parser
        .s_iter(s_tag);

    let base_uuid = Uuid::from_str(base).unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(output)
        .unwrap();

    let layer = SegmentationLayer::encode_to_file(file, values, length, "bla".to_owned(), base_uuid, compressed, comment);
    (layer.len(), layer.header.uuid().to_string())
}

#[pyfunction]
fn encode_ptr_from_p(input: &str, basecol: usize, headcol: usize, length: usize, base: &str, compressed: bool, comment: &str, output: &str) -> usize {
    let tails = open_reader(input).unwrap().iter_p(basecol);
    let heads = open_reader(input).unwrap().iter_p(headcol);

    let values = tails.zip(heads).map(|((cpos, base), (_, head))| {
        if let Ok(h) = head.parse::<i64>() {
            if let Ok(b) = base.parse::<i64>() {
                if h == 0 {
                    return cpos as i64;
                } else {
                    return cpos as i64 + (h - b);
                }
            }
        }
        -1
    });

    let base_uuid = Uuid::from_str(base).unwrap();

    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(output)
        .unwrap();

    let variable = PointerVariable::encode_to_file(file, values, length, "".to_owned(), base_uuid, compressed, comment);
    variable.len()
}

#[pyfunction]
fn vrt_stats(input: &str) -> (usize, usize, HashMap<String, usize>) {
    let mut reader = open_reader(input).unwrap();
    reader.stats()
}

pub struct PIter<R: Read> {
    reader: VrtReader<R>,
    column: usize,
}

impl<R: Read> Iterator for PIter<R> {
    type Item = (usize, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next_p(self.column).map(|(i, s)| (i, s.to_string()))
    }
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

#[derive(Debug)]
pub enum ReaderEvent<'a> {
    Line(usize),
    TagOpen(usize, &'a str),
    TagClose(usize, &'a str),
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

    pub fn last_line(&self) -> &str {
        &self.last_line
    }

    pub fn read_next(&mut self) -> Option<ReaderEvent> {
        self.last_line.clear();
        match self.reader.read_line(&mut self.last_line) {
            Ok(0) => None,

            Ok(_) => {
                let mut line = self.last_line.trim();
                if line.starts_with("</") {
                    line = line.trim_start_matches("</");
                    line = line.split_whitespace().next().unwrap();
                    line = line.trim_end_matches('>');
                    Some(ReaderEvent::TagClose(self.cpos, line))
                } else if line.starts_with('<') {
                    line = line.trim_start_matches('<');
                    line = line.split_whitespace().next().unwrap();
                    line = line.trim_end_matches('>');
                    Some(ReaderEvent::TagOpen(self.cpos, line))
                } else {
                    let value = ReaderEvent::Line(self.cpos);
                    self.cpos += 1;
                    Some(value)
                }
            }

            Err(_) => None,
        }
    }

    pub fn next_p(&mut self, column: usize) -> Option<(usize, &str)> {
        while let Some(event) = self.read_next() {
            match event {
                ReaderEvent::Line(cpos) => {
                    return self.last_line.trim()
                        .split('\t')
                        .nth(column)
                        .map(| token | (cpos, token))
                }

                _ => continue,
            }
        }
        None
    }

    pub fn stats(&mut self) -> (usize, usize, HashMap<String, usize>) {
        let mut pcount = 0;
        let mut scounts: HashMap<String, usize> = HashMap::new();

        while let Some(event) = self.read_next() {
            match event {
                crate::ReaderEvent::Line(cpos) => {
                    if cpos == 0 {
                        pcount = self.last_line.split('\t').count();
                    }
                }

                crate::ReaderEvent::TagOpen(_, _) => (),

                crate::ReaderEvent::TagClose(_, tag) =>  {
                    let count = scounts.entry(tag.to_owned()).or_insert_with(|| 0);
                    *count += 1;
                }
            }
        }

        (self.cpos, pcount, scounts)
    }

    pub fn iter_p(self, column: usize) -> PIter<R> {
        PIter { reader: self, column}
    }
}

pub fn open_reader(filename: &str) -> IoResult<VrtReader<Box<dyn Read>>> {
    let file = File::open(filename)?;
    if filename.ends_with("gz") {
        Ok(VrtReader::new(Box::new(MultiGzDecoder::new(file))))
    } else {
        Ok(VrtReader::new(Box::new(file)))
    }
}

pub fn open_parser(filename: &str) -> IoResult<VrtParser<Box<dyn Read>>> {
    let file = File::open(filename)?;
    if filename.ends_with("gz") {
        Ok(VrtParser::new(Box::new(MultiGzDecoder::new(file))))
    } else {
        Ok(VrtParser::new(Box::new(file)))
    }
}

#[derive(Debug)]
pub enum ParserEvent {
    PLine(usize, String),
    SAttr(usize, usize, String, HashMap<String, String>),
}

pub struct VrtParser<R: Read> {
    xml: Reader<BufReader<R>>,
    buffer: Vec<u8>,
    cpos: usize,
    lines: VecDeque<String>,
    lpos: usize,
    ltotal: usize,
    stack: Vec<(usize, String, HashMap<String, String>)>,
}

impl<R: Read> VrtParser<R> {
    pub fn new(readable: R) -> Self {
        let bufreader = BufReader::new(readable);
        let mut reader = Reader::from_reader(bufreader);
        reader.trim_text(true);

        Self {
            xml: reader,
            buffer: Vec::new(),
            cpos: 0,
            lines: VecDeque::new(),
            lpos: 0,
            ltotal: 0,
            stack: Vec::new(),
        }
    }

    fn read_next(&mut self) -> Option<ParserEvent> {
        // if there are lines in the buffer return them as individual line events
        if self.lpos < self.ltotal {
            let line = self.lines.pop_front().unwrap();
            let attr = ParserEvent::PLine(self.cpos, line);
            self.cpos += 1;
            self.lpos += 1;
            return Some(attr);
        }

        // line buffer done
        self.lpos = 0;
        self.ltotal = 0;
        self.lines.clear(); // line buffer
        self.buffer.clear(); // event buffer

        while let Some(event) = self.xml.read_event_into(&mut self.buffer).ok() {
            // process next XML event
            match event {
                Event::Start(s) => {
                    // copy tag name and attributes and put it on the parse stack
                    let name = String::from_utf8(s.local_name().into_inner().to_owned()).unwrap();
                    let attrs: Result<HashMap<String, String>, _> = s.attributes().map(| res | {
                        res.map(| attr | {
                            let key = String::from_utf8(attr.key.local_name().into_inner().to_owned()).unwrap();
                            let value = attr.decode_and_unescape_value(&mut self.xml).unwrap().to_string();
                            (key, value)
                        })
                    })
                    .collect();
                    
                    self.stack.push((self.cpos, name, attrs.unwrap()));
                    continue
                }

                Event::End(e) => {
                    // try close last tag from the stack and return event
                    if let Some((start, name, attrs)) = self.stack.pop() {
                        // if the last start tag returned from the stack does not match the current end tag
                        // we have invalid xml. <a><b></a></b> cannot be possible.
                        assert!(e.local_name().into_inner() == name.as_bytes(), "unclosed S attr");
                        return Some(ParserEvent::SAttr(start, self.cpos, name, attrs))
                    }
                    panic!("encountered end tag before first start tag");
                }

                Event::Text(t) => {
                    // split text into lines and push them into the line buffer
                    for l in t.lines() {
                        self.lines.push_back(l.unwrap());
                    }
                    // this is fine because this code cannot be reached if lpos/ltotal > 0
                    self.ltotal = self.lines.len();
                    self.lpos = 1; // we issue the first line event from this code block
                    let attr = ParserEvent::PLine(self.cpos, self.lines.pop_front().unwrap());
                    self.cpos += 1;

                    return Some(attr)
                }

                Event::Eof => return None,

                _ => continue,
            };
        }

        None
    }

    pub fn next_p(&mut self, column: usize) -> Option<(usize, String)> {
        while let Some(event) = self.read_next() {
            match event {
                ParserEvent::PLine(cpos, line) => {
                    let value = line.split('\t').nth(column)?;
                    return Some((cpos, value.to_owned()));
                }

                _ => continue,
            }
        }

        None
    }

    pub fn next_s(&mut self, tag: &str) -> Option<(usize, usize)> {
        while let Some(event) = self.read_next() {
            match event {
                ParserEvent::SAttr(start, end, name, _) => {
                    if name == tag {
                        return Some((start, end))
                    }
                }

                _ => continue,
            }
        }

        None
    }

    pub fn next_a(&mut self, tag: &str, attr: &str) -> Option<(usize, usize, String)> {
        while let Some(event) = self.read_next() {
            match event {
                ParserEvent::SAttr(start, end, name, mut attrs) => {
                    if name == tag {
                        return Some((start, end, attrs.remove(attr).unwrap()))
                    }
                }

                _ => continue,
            }
        }

        None
    }

    pub fn a_iter(self, tag: &str, attr: &str) -> AIter<R> {
        AIter { 
            tag: tag.to_string(),
            attr: attr.to_string(),
            parser: self,
        }
    }

    pub fn s_iter(self, tag: &str) -> SIter<R> {
        SIter { 
            tag: tag.to_string(),
            parser: self,
        }
    }
}

pub struct SIter<R: Read> {
    tag: String,
    parser: VrtParser<R>,
}

impl<R: Read> Iterator for SIter<R> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        self.parser.next_s(&self.tag)
    }
}

pub struct AIter<R: Read> {
    tag: String,
    attr: String,
    parser: VrtParser<R>,
}

impl<R: Read> Iterator for AIter<R> {
    type Item = (usize, usize, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.parser.next_a(&self.tag, &self.attr)
    }
}

#[cfg(test)]
mod tests {
    use test::{Bencher, black_box};
    use crate::open_reader;
    use crate::open_parser;

    #[test]
    fn it_works() {
        let mut file = open_reader("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        file.read_next();
    }

    #[test]
    fn read_events() {
        let mut file = open_reader("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        while let Some(event) = file.read_next() {
            println!("{:?}", event);
        }
    }

    #[test]
    fn it_works_parser() {
        let mut file = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        file.read_next();
    }

    #[test]
    fn read_events_parser() {
        let mut file = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        println!();
        while let Some(event) = file.read_next() {
            match event {
                crate::ParserEvent::PLine(cpos, line) => println!("{}: {}", cpos, line),
                crate::ParserEvent::SAttr(start, end, tag, attrs) => println!("<{}, {}, {}, {:?}>", tag, start, end, attrs),
            }
        }
    }

    #[test]
    fn read_p_comp() {
        let mut file1 = open_reader("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        let mut file2 = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();

        while let Some(((cpos1, v1), (cpos2, v2))) = file1.next_p(0).zip(file2.next_p(0)) {
            assert!(cpos1 == cpos2, "discrepancy in cpos");
            assert!(v1 == v2, "discrepancy in value");
        }
    }

    #[test]
    fn read_s_parser() {
        let mut file = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        println!();
        while let Some((start, end)) = file.next_s("s") {
            println!("text {}, {}", start, end);
        }
    }

    #[test]
    fn read_a_parser() {
        let mut file = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        println!();
        while let Some((start, end, value)) = file.next_a("text", "id") {
            println!("text_id {}, {}: {}", start, end, value);
        }
    }

    #[bench]
    fn bench_read_p(b: &mut Bencher) {
        b.iter(||{
            let mut file = open_reader("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
            while let Some(attr) = file.next_p(0) {
                black_box(attr);
            }
        });
    }

    #[bench]
    fn bench_read_p_parser(b: &mut Bencher) {
        b.iter(||{
            let mut file = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
            while let Some(attr) = file.next_p(0) {
                black_box(attr);
            }
        });
    }

    #[bench]
    fn bench_read_s_parser(b: &mut Bencher) {
        b.iter(||{
            let mut file = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
            while let Some(attr) = file.next_s("s") {
                black_box(attr);
            }
        });
    }

    #[bench]
    fn bench_read_a_parser(b: &mut Bencher) {
        b.iter(||{
            let mut file = open_parser("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
            while let Some(attr) = file.next_a("text", "id") {
                black_box(attr);
            }
        });
    }

    #[test]
    fn vrt_stats() {
        let mut reader = open_reader("../etemenanki/testdata/Dickens-1.0.xml.gz").unwrap();
        let (clen, pcount, scounts) = reader.stats();

        println!("\nCorpus with {} positions and {} P attrs", clen, pcount);
        println!("S attrs: {:?}", scounts);
    }
}
