#![feature(test)]

use core::fmt;
use std::{
    error::Error,
    ffi::{CStr, CString},
    path::Path,
};

use num_enum::TryFromPrimitive;

use bindings::*;

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
    pub fn new<P: AsRef<Path>>(registry_dir: P, registry_name: &str) -> Option<Corpus> {
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
                None
            } else {
                Some(Corpus { ptr: c })
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
            cl_delete_string_list(attrs);
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

    pub fn get_s_attribute(&self, name: &str) -> Option<StructuralAttribute> {
        let cname = CString::new(name).unwrap();
        unsafe {
            let attr = cl_new_attribute(self.ptr, cname.as_ptr(), bindings::ATT_STRUC as i32);
            if attr.is_null() {
                None
            } else {
                Some(StructuralAttribute {
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

#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive)]
#[repr(i32)]
pub enum DataAccessError {
    OK = 0,                    // everything is fine; actual error values are all less than 0
    ENULLATT = -1,             // NULL passed as attribute argument
    EATTTYPE = -2,             // function was called on illegal attribute
    EIDORNG = -3,              // id out of range
    EPOSORNG = -4,             // position out of range
    EIDXORNG = -5,             // index out of range
    ENOSTRING = -6,            // no such string encoded
    EPATTERN = -7,             // illegal pattern
    ESTRUC = -8,               // no structure at position
    EALIGN = -9,               // no alignment at position
    EREMOTE = -10,             // error in remote access
    ENODATA = -11,             // can't load/create necessary data
    EARGS = -12,               // error in arguments for dynamic call or CL function
    ENOMEM = -13,              // memory fault [unused]
    EOTHER = -14,              // other error
    ENYI = -15,                // not yet implemented
    EBADREGEX = -16,           // bad regular expression
    EFSETINV = -17,            // invalid feature set format
    EBUFFER = -18,             // buffer overflow (hard-coded internal buffer sizes)
    EINTERNAL = -19,           // internal data consistency error (really bad)
    EACCESS = -20,             // insufficient access permissions
    EPOSIX = -21,              // POSIX-level error: check errno or perror()
    CPOSUNDEF = std::i32::MIN, // undefined corpus position (use this code to avoid ambiguity with negative cpos)
}

impl fmt::Display for DataAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = unsafe {
            let ptr = cl_error_string(*self as i32);
            ptr_to_str(ptr)
        };
        match str {
            Some(str) => f.write_str(str),
            None => f.write_str("Unknown Error"),
        }
    }
}

impl Error for DataAccessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

macro_rules! cl_error_or {
    ($ok:expr) => {{
        let error = DataAccessError::try_from(cl_errno).expect("invalid cl_errno value");
        match error {
            DataAccessError::OK => Ok($ok),
            _ => Err(error),
        }
    }};
}

pub type AccessResult<T> = Result<T, DataAccessError>;

#[derive(Debug)]
pub struct MallocSlice<'c, T> {
    inner: &'c [T],
}

impl<'c, T> MallocSlice<'c, T> {
    pub unsafe fn from_raw_parts(data: *const T, len: usize) -> Self {
        Self {
            inner: std::slice::from_raw_parts(data, len),
        }
    }
}

impl<'c, T> std::ops::Deref for MallocSlice<'c, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'c, T> std::ops::Drop for MallocSlice<'c, T> {
    fn drop(&mut self) {
        unsafe {
            let ptr = self.inner.as_ptr() as *mut libc::c_void;
            libc::free(ptr);
        }
    }
}

pub struct PositionalAttribute<'c> {
    ptr: *mut bindings::Attribute,
    _parent: &'c Corpus,
}

impl<'c> PositionalAttribute<'c> {
    pub fn id2str(&self, id: i32) -> AccessResult<&'c CStr> {
        unsafe {
            let ptr = cl_id2str(self.ptr, id);
            cl_error_or!(CStr::from_ptr(ptr))
        }
    }

    pub fn str2id(&self, str: &CStr) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_str2id(self.ptr, str.as_ptr() as *mut i8)) }
    }

    pub fn id2strlen(&self, id: i32) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_id2strlen(self.ptr, id)) }
    }

    pub fn sort2id(&self, sort_index_position: i32) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_sort2id(self.ptr, sort_index_position)) }
    }

    pub fn id2sort(&self, id: i32) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_id2sort(self.ptr, id)) }
    }

    pub fn max_cpos(&self) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_max_cpos(self.ptr)) }
    }

    pub fn max_id(&self) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_max_id(self.ptr)) }
    }

    pub fn id2freq(&self, id: i32) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_id2freq(self.ptr, id)) }
    }

    pub fn id2cpos(&self, id: i32) -> AccessResult<MallocSlice<i32>> {
        unsafe {
            let mut freq = 0;
            let ptr = cl_id2cpos_oldstyle(self.ptr, id, &mut freq, core::ptr::null_mut(), 0);
            cl_error_or!(MallocSlice::from_raw_parts(ptr, freq as usize))
        }
    }

    pub fn cpos2id(&self, position: i32) -> AccessResult<i32> {
        unsafe { cl_error_or!(cl_cpos2id(self.ptr, position)) }
    }

    pub fn cpos2str(&self, position: i32) -> AccessResult<&'c CStr> {
        unsafe {
            let ptr = cl_cpos2str(self.ptr, position);
            cl_error_or!(CStr::from_ptr(ptr))
        }
    }

    pub fn id2all(&self, id: i32) -> AccessResult<(&'c CStr, i32, i32)> {
        unsafe {
            let mut slen = 0;
            let mut freq = 0;
            let ptr = cl_id2all(self.ptr, id, &mut freq, &mut slen);
            cl_error_or!((CStr::from_ptr(ptr), slen, freq))
        }
    }

    pub fn regex2id(&self, pattern: &CStr, flags: i32) -> AccessResult<MallocSlice<i32>> {
        unsafe {
            let mut len = 0;
            let ptr = cl_regex2id(self.ptr, pattern.as_ptr() as *mut i8, flags, &mut len);
            if ptr.is_null() {
                // this is a hack for getting a freeable MallocSlice with size 0 when the
                // LibCL returns a null pointer
                // Rust slices mustn't use a null pointer for their data pointer
                cl_error_or!(MallocSlice::from_raw_parts(libc::malloc(1) as *mut i32, len as usize))
            } else {
                cl_error_or!(MallocSlice::from_raw_parts(ptr, len as usize))
            }
        }
    }

    pub fn idlist2freq(&self, idlist: &[i32]) -> AccessResult<i32> {
        unsafe {
            let freq = cl_idlist2freq(self.ptr, idlist.as_ptr() as *mut _, idlist.len() as i32);
            if freq < 0 {
                let error = DataAccessError::try_from(freq).expect("invalid cl_errno value");
                match error {
                    DataAccessError::OK => unreachable!("freq must be negative here"),
                    _ => Err(error),
                }
            } else {
                Ok(freq)
            }
        }
    }

    pub fn idlist2cpos(&self, idlist: &[i32], sort: bool) -> AccessResult<MallocSlice<i32>> {
        unsafe {
            let mut len = 0;
            let ptr = cl_idlist2cpos_oldstyle(
                self.ptr,
                idlist.as_ptr() as *mut _,
                idlist.len() as i32,
                sort as i32,
                &mut len,
                core::ptr::null_mut(),
                0,
            );
            if ptr.is_null() {
                // this is a hack for getting a freeable MallocSlice with size 0 when the
                // LibCL returns a null pointer
                // Rust slices mustn't use a null pointer for their data pointer
                cl_error_or!(MallocSlice::from_raw_parts(libc::malloc(1) as *mut i32, len as usize))
            } else {
                cl_error_or!(MallocSlice::from_raw_parts(ptr, len as usize))
            }
        }
    }
}

pub struct StructuralAttribute<'c> {
    ptr: *mut bindings::Attribute,
    _parent: &'c Corpus,
}

impl<'c> StructuralAttribute<'c> {
    pub fn cpos2struc2cpos(&self, position: i32) -> AccessResult<(i32, i32)> {
        unsafe {
            let mut start = 0;
            let mut end = 0;
            cl_cpos2struc2cpos(self.ptr, position, &mut start, &mut end);
            cl_error_or!((start, end))
        }
    }

    pub fn cpos2struc(&self, cpos: i32) -> AccessResult<i32> {
        unsafe {
            cl_error_or!(cl_cpos2struc(self.ptr, cpos))
        }
    }

    pub fn cpos2boundary(&self, cpos: i32) -> AccessResult<u32> {
        unsafe {
            let boundary = cl_cpos2boundary(self.ptr, cpos);
            if boundary >= 0 {
                Ok(boundary as u32)
            } else {
                Err(DataAccessError::try_from(boundary).unwrap())
            }
        }
    }

    pub fn struc2cpos(&self, struc_num: i32) -> AccessResult<(i32, i32)> {
        unsafe {
            let mut start = 0;
            let mut end = 0;
            cl_struc2cpos(self.ptr, struc_num, &mut start, &mut end);
            cl_error_or!((start, end))
        }
    }

    pub fn max_struc(&self) -> AccessResult<i32> {
        unsafe {
            cl_error_or!(cl_max_struc(self.ptr))
        }
    }

    pub fn struc_values(&self) -> AccessResult<bool> {
        unsafe {
            cl_error_or!(cl_struc_values(self.ptr) != 0)
        }
    }

    pub fn struc2str(&self, struc_num: i32) -> AccessResult<&'c CStr> {
        unsafe {
            if self.struc_values()? {
                let ptr = cl_struc2str(self.ptr, struc_num);
                cl_error_or!(CStr::from_ptr(ptr))
            } else {
                Err(DataAccessError::ENOSTRING)
            }
        }
    }

    pub fn cpos2struc2str(&self, position: i32) -> AccessResult<&'c CStr> {
        unsafe {
            let ptr = cl_struc2str(self.ptr, position);
            cl_error_or!(CStr::from_ptr(ptr))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_corpus() {
        let _ = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");
    }

    #[test]
    fn corpus_props() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let props = c.get_properties();
        assert!(props.len() == 2);
        assert!(props[0] == ("language", "en"));
        assert!(props[1] == ("charset", "utf8"));
    }

    #[test]
    fn list_attrs() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

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
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let word = c.get_p_attribute("word").unwrap();
        assert!((word.max_cpos().unwrap(), word.max_id().unwrap()) == (3407085, 57568));

        let pos = c.get_p_attribute("pos").unwrap();
        assert!((pos.max_cpos().unwrap(), pos.max_id().unwrap()) == (3407085, 43));

        let lemma = c.get_p_attribute("lemma").unwrap();
        assert!((lemma.max_cpos().unwrap(), lemma.max_id().unwrap()) == (3407085, 41222));
    }

    #[test]
    fn lexicon() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let word = c.get_p_attribute("word").unwrap();

        let size = word.max_id().unwrap();
        for i in 0..size {
            word.id2str(i).unwrap();
        }
    }

    #[test]
    fn malloc_slice() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let word = c.get_p_attribute("word").unwrap();

        for i in 1337..2000 {
            let slc = word.id2cpos(i).unwrap();
            assert!(slc[0] > 0 && slc.len() > 0)
        } // slc should be properly dropped here
    }

    #[test]
    fn simple_query() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let word = c.get_p_attribute("word").unwrap();

        let ids = word
            .regex2id(&CString::new(r"author.+s").unwrap(), 0)
            .unwrap();

        let matches = word.idlist2cpos(&ids, true).unwrap();
        assert!(matches.len() == 45);
        assert!(matches.len() == word.idlist2freq(&ids).unwrap() as usize);
        assert!(matches[0] == 70993);

        println!("\nMatches:");
        for i in matches.iter() {
            print!("{}: ", i);
            for c in i - 5..i + 6 {
                if c == *i {
                    print!("<{}> ", word.cpos2str(c).unwrap().to_str().unwrap());
                } else {
                    print!("{} ", word.cpos2str(c).unwrap().to_str().unwrap());
                }
            }
            println!();
        }

        println!("Freqs:");
        for id in ids.iter() {
            let token = word.id2str(*id).unwrap().to_str().unwrap();
            let freq = word.id2freq(*id).unwrap();
            println!("{}: {}", token, freq);
        }
    }

    #[test]
    fn zero_size_lists() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let word = c.get_p_attribute("word").unwrap();

        let idlist = vec![];

        let freq = word.idlist2freq(&idlist).unwrap();
        assert!(freq == 0);

        let cpos = word.idlist2cpos(&idlist, true).unwrap();
        assert!(cpos.len() == 0);

        let matches = word.regex2id(&CString::new(r"xlaoiersntyuoanrst").unwrap(), 0).unwrap();
        assert!(matches.len() == 0);
    }

    #[test]
    fn open_sattrs() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let expected = vec![
            (14, false),
            (14, true),
            (14, false),
            (14, true),
            (696, false),
            (696, true),
            (696, true),
            (61177, false),
            (152455, false),
        ];

        for (name, (max, has_vals)) in c.list_s_attributes().iter().zip(expected.iter()) {
            let sattr = c.get_s_attribute(name).unwrap();
            assert!(sattr.max_struc().unwrap() == *max);
            assert!(sattr.struc_values().unwrap() == *has_vals);
        }
    }

    #[test]
    fn struc2str_no_values() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let text = c.get_s_attribute("text").unwrap();

        let str = text.struc2str(0);
        assert!(str == Err(DataAccessError::ENOSTRING));
    }

    #[test]
    fn decode_sattr_values() {
        let c = Corpus::new("testdata/registry", "simpledickens").expect("Could not open corpus");

        let chapter_title = c.get_s_attribute("chapter_title").unwrap();
        let len = chapter_title.max_struc().unwrap();

        println!();
        for i in 0..len {
            let _ = chapter_title.struc2str(i).unwrap();
        }
    }
}
