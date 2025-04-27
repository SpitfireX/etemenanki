//! Common helper types for writing CL hooks.
//! This includes Rust FFI equivalents of native CL objects.

use std::ffi::CStr;
use libc::{c_char, c_int};

/// Helper type for marking C-style output parameters on logging hooked functions.
/// The wrapper type offers no functionality itself and acts as a `T` in every way.
/// 
/// Marked parameters are treated as return values and their values is dereferenced and logged.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Out<T> (T);

impl<T> std::ops::Deref for Out<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Out<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Helper type mapping the first members of the CL's TCorpus struct type
#[repr(C)]
pub struct Corpus {
    id: *mut c_char,
    name: *mut c_char,
    path: *mut c_char,
    info_file: *mut c_char,
}

impl std::fmt::Display for Corpus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            write!(f, "Corpus({:?})", CStr::from_ptr(self.id))
        }
    }
}

/// Helper struct mapping some of the members of the CL's _Attribute union type.
/// Only the members from the `COMMON_ATTR_FIELDS` macro can be accessed via this struct.
#[repr(C)]
pub struct Attribute {
    attr_type: c_int,
    name: *mut c_char,
    next: *mut Attribute,
    attr_number: c_int,
    path: *mut c_char,
    mother: *mut Corpus,
}

impl std::fmt::Display for Attribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            write!(f, "Attribute({:?}, {})", CStr::from_ptr(self.name), self.attr_number)
        }
    }
}
