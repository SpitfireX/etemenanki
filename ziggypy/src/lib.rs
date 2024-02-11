use std::{fs::File, io::{BufRead, BufReader, Read, Result as IoResult}};
use flate2::read::GzDecoder;
use xml::{name::OwnedName, reader::{EventReader, XmlEvent}};

use pyo3::prelude::*;

#[pymodule]
#[pyo3(name="_rustypy")]
fn module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<IndexedStringCore>()?;
    Ok(())
}

#[pyclass]
struct IndexedStringCore {
    length: usize,
}

#[pymethods]
impl IndexedStringCore {
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

fn open_file(filename: &str) -> IoResult<VrtReader<Box<dyn Read>>> {
    let file = File::open(filename)?;
    if filename.ends_with("gz") {
        Ok(VrtReader::new(Box::new(GzDecoder::new(file))))
    } else {
        Ok(VrtReader::new(Box::new(file)))
    }
}

enum ReaderState {
    Next,
    Text(Vec<String>, usize),
    Segment(OwnedName, usize, usize),
    End,
}

struct VrtReader<R: Read> {
    xml_reader: EventReader<BufReader<R>>,
    cpos: usize,
    stack: Vec<(OwnedName, usize)>,
    state: ReaderState,
}

impl<R: Read> VrtReader<R> {
    pub fn new(readable: R) -> Self {
        Self { 
            xml_reader: EventReader::new(BufReader::new(readable)),
            cpos: 0,
            stack: Vec::new(),
            state: ReaderState::Next,
        }
    }

    fn read_next(&mut self) {
        self.state = match self.xml_reader.next() {
            Ok(event) => match event {
                XmlEvent::StartElement { name, attributes: _, namespace: _ } => {
                    self.stack.push((name, self.cpos));
                    ReaderState::Next
                }

                XmlEvent::EndElement { name } => {
                    println!("end: {} at {}", name, self.cpos);
                    self.stack.last()
                        .filter(| (tag, _) | dbg!(*tag == name))
                        .map(| (_, start) | ReaderState::Segment(name, *start, self.cpos))
                        .unwrap_or(ReaderState::End)
                }

                XmlEvent::Characters(text) => ReaderState::Text(text.trim().lines().map(String::from).collect(), 0),

                XmlEvent::EndDocument => ReaderState::End,
                _ => ReaderState::Next,
            }

            Err(_) => ReaderState::End,
        };
    }

    fn next_p(&mut self, column: usize) -> Option<(usize, String)> {
        loop {
            match &mut self.state {
                ReaderState::Text(lines, pos) => {
                    if *pos < lines.len() {
                        let line = &lines[*pos];
                        return line.split('\t')
                            .nth(column)
                            .map(| v | (self.cpos, v.to_owned()))
                            .inspect(| _ | self.cpos += 1)
                            .inspect(| _ | *pos += 1);
                    } else {
                        self.state = ReaderState::Next;
                    }
                }
                _ => {
                    match self.state {
                        ReaderState::End => return None,
                        _ => self.read_next(),
                    }
                }
            }
        }
    }

    fn next_s(tag: &str) -> Option<((usize, usize))> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use crate::open_file;

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
}
