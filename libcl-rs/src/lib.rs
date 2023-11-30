#![feature(test)]

use std::{
    ffi::{CStr, CString},
    path::Path,
};

use bindings::{
    cl_corpus_list_attributes, cl_delete_corpus, cl_first_corpus_property, cl_max_cpos, cl_max_id,
    cl_new_attribute, cl_string_list_get, cl_string_list_size, Attribute,
};

mod bindings {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

    #[cfg(test)]
    mod tests {
        extern crate test;

        use super::*;

        use libc::free;
        use std::ffi::CString;
        use test::Bencher;

        #[test]
        fn malloc() {
            unsafe {
                let test = "Hello malloc!";
                let len = test.as_bytes().len();
                let ptr = cl_malloc(len);
                ptr.copy_from(test.as_ptr() as *const std::os::raw::c_void, len);

                let s =
                    std::str::from_utf8(std::slice::from_raw_parts(ptr as *const u8, len)).unwrap();
                assert!(s == test);

                free(ptr);
            }
        }

        #[test]
        fn create_destroy_string() {
            unsafe {
                let astr = cl_autostring_new("Hello\0".as_bytes().as_ptr() as *const i8, 6);
                cl_autostring_concat(astr, " World!\0".as_bytes().as_ptr() as *mut i8);
                assert!(cl_autostring_len(astr) == 12);
                cl_autostring_delete(astr);
            }
        }

        #[test]
        fn open_corpus() {
            unsafe {
                // open test corpus
                let path = CString::new("testdata/registry").unwrap();
                let name = CString::new("simpledickens").unwrap();
                let c = cl_new_corpus(path.as_ptr() as *mut i8, name.as_ptr() as *mut i8);
                assert!(!c.is_null());

                // check for attributes
                let attrs = cl_corpus_list_attributes(c, ATT_ALL as i32);
                assert!(!attrs.is_null());
                assert!(cl_string_list_size(attrs) > 0);
                cl_delete_string_list(attrs);

                cl_delete_corpus(c);
            }
        }

        #[bench]
        fn seqdecode(b: &mut Bencher) {
            unsafe {
                // open test corpus
                let path = CString::new("testdata/registry").unwrap();
                let name = CString::new("simpledickens").unwrap();
                let c = cl_new_corpus(path.as_ptr() as *mut i8, name.as_ptr() as *mut i8);
                assert!(!c.is_null());

                // open p attribute
                let attr = cl_new_attribute(c, "word\0".as_ptr() as *const i8, ATT_POS as i32);
                assert!(!attr.is_null());
                assert!(cl_attribute_mother_corpus(attr) == c);

                let max = cl_max_cpos(attr);
                assert!(max > 0);

                let mut len = 0;

                // decode complete attribute
                b.iter(|| {
                    for i in 0..max {
                        let str = cl_cpos2str(attr, i);
                        len += libc::strlen(str);
                    }
                });

                println!("total chars: {}", len);
            }
        }
    }
}

unsafe fn ptr_to_str<'c>(ptr: *mut i8) -> Option<&'c str> {
    if ptr.is_null() {
        None
    } else {
        let cs = CStr::from_ptr(ptr);
        match cs.to_str() {
            Ok(str) => Some(str),
            Err(_) => None,
        }
    }
}

unsafe fn ptr_to_str_unchecked<'c>(ptr: *mut i8) -> Option<&'c str> {
    if ptr.is_null() {
        None
    } else {
        let cs = CStr::from_ptr(ptr);
        let bytes = cs.to_bytes();
        Some(std::str::from_utf8_unchecked(&bytes))
    }
}

pub struct Corpus {
    ptr: *mut bindings::Corpus,
}

impl Corpus {
    pub fn open<P: AsRef<Path>>(registry_dir: P, registry_name: &str) -> Result<Corpus, ()> {
        let dir = CString::new(
            registry_dir
                .as_ref()
                .to_str()
                .unwrap()
                .trim_end_matches("/"),
        )
        .unwrap();
        let name = CString::new(registry_name).unwrap();

        unsafe {
            let c = bindings::cl_new_corpus(dir.as_ptr() as *mut i8, name.as_ptr() as *mut i8);

            if c.is_null() {
                Err(())
            } else {
                Ok(Corpus { ptr: c })
            }
        }
    }

    pub fn get_properties(&self) -> Vec<(&str, &str)> {
        let mut props = Vec::new();
        unsafe {
            let mut prop = cl_first_corpus_property(self.ptr);
            while !prop.is_null() {
                let k = ptr_to_str((*prop).property).unwrap();
                let v = ptr_to_str((*prop).value).unwrap();
                props.push((k, v));
                prop = (*prop).next;
            }
        }

        props
    }

    pub fn list_attributes(&self, attr_type: i32) -> Vec<&str> {
        let mut names = Vec::new();
        unsafe {
            let attrs = cl_corpus_list_attributes(self.ptr, attr_type);
            if !attrs.is_null() {
                for i in 0..cl_string_list_size(attrs) {
                    let sptr = cl_string_list_get(attrs, i);
                    if let Some(str) = ptr_to_str(sptr) {
                        names.push(str);
                    }
                }
            }
        }

        names
    }

    pub fn list_p_attributes(&self) -> Vec<&str> {
        self.list_attributes(bindings::ATT_POS as i32)
    }

    pub fn list_s_attributes(&self) -> Vec<&str> {
        self.list_attributes(bindings::ATT_STRUC as i32)
    }

    pub fn get_p_attribute(&self, name: &str) -> Option<PositionalAttribute> {
        let cname = CString::new(name).unwrap();
        unsafe {
            let attr = cl_new_attribute(self.ptr, cname.as_ptr(), bindings::ATT_POS as i32);
            if attr.is_null() {
                None
            } else {
                Some(PositionalAttribute {
                    ptr: attr,
                    _parent: self,
                })
            }
        }
    }
}

impl Drop for Corpus {
    fn drop(&mut self) {
        unsafe {
            cl_delete_corpus(self.ptr);
        }
    }
}

pub struct PositionalAttribute<'c> {
    ptr: *mut bindings::Attribute,
    _parent: &'c Corpus,
}

impl<'c> PositionalAttribute<'c> {
    pub fn max_cpos(&self) -> usize {
        unsafe { cl_max_cpos(self.ptr) as usize }
    }

    pub fn max_id(&self) -> usize {
        unsafe { cl_max_id(self.ptr) as usize }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_corpus() {
        let _ = Corpus::open("testdata/registry", "simpledickens").expect("Could not open corpus");
    }

    #[test]
    fn corpus_props() {
        let c = Corpus::open("testdata/registry", "simpledickens").expect("Could not open corpus");

        let props = c.get_properties();
        assert!(props.len() == 2);
        assert!(props[0] == ("language", "en"));
        assert!(props[1] == ("charset", "utf8"));
    }

    #[test]
    fn list_attrs() {
        let c = Corpus::open("testdata/registry", "simpledickens").expect("Could not open corpus");

        let pattrs = c.list_p_attributes();
        assert!(pattrs == ["word", "pos", "lemma"]);

        let sattrs = c.list_s_attributes();
        assert!(
            sattrs
                == [
                    "text",
                    "text_id",
                    "novel",
                    "novel_title",
                    "chapter",
                    "chapter_num",
                    "chapter_title",
                    "p",
                    "s"
                ]
        );

        let nope = c.list_attributes(0);
        assert!(nope.len() == 0);
    }

    #[test]
    fn open_pattrs() {
        let c = Corpus::open("testdata/registry", "simpledickens").expect("Could not open corpus");

        let word = c.get_p_attribute("word").unwrap();
        assert!((word.max_cpos(), word.max_id()) == (3407085, 57568));

        let pos = c.get_p_attribute("pos").unwrap();
        assert!((pos.max_cpos(), pos.max_id()) == (3407085, 43));

        let lemma = c.get_p_attribute("lemma").unwrap();
        assert!((lemma.max_cpos(), lemma.max_id()) == (3407085, 41222));

    }
}
