use libcl_hook_macros::{hook, logged_hook};
use core::fmt;
use std::ffi::CStr;
use libc::{c_char, c_int, c_void};

#[repr(C)]
struct Corpus {
    id: *mut c_char,
    name: *mut c_char,
    path: *mut c_char,
    info_file: *mut c_char,
}

impl fmt::Display for Corpus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            write!(f, "Corpus({:?}, {:?})", CStr::from_ptr(self.path), CStr::from_ptr(self.id))
        }
    }
}

#[repr(C)]
#[derive(Debug)]
struct Attribute {
    attr_type: c_int,
    name: *mut c_char,
    next: *mut Attribute,
    attr_number: c_int,
    path: *mut c_char,
    mother: *mut Corpus,
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            write!(f, "Attribute({:?}, {})", CStr::from_ptr(self.name), self.attr_number)
        }
    }
}


#[logged_hook]
fn cl_new_corpus(registry_dir: *mut c_char, registry_name: *mut c_char) -> *mut Corpus {}

#[logged_hook]
fn cl_new_attribute(corpus: *mut c_void, attribute_name: *mut c_char, attr_type: c_int) -> *mut Attribute {}

#[logged_hook]
fn cl_id2str(attribute: *mut Attribute, id: c_int) -> *mut c_char {}
