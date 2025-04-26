use libcl_hook_macros::{hook, logged_hook};
use std::ffi::CStr;
use libc::{c_char, c_int, c_void};

#[logged_hook]
fn cl_new_corpus(registry_dir: *mut c_char, registry_name: *mut c_char) -> *mut c_void {}

#[logged_hook]
fn cl_new_attribute(corpus: *mut c_void, attribute_name: *mut c_char, attr_type: c_int) -> *mut c_void {}

#[logged_hook]
fn cl_id2str(attribute: *mut c_void, id: c_int) -> *mut c_char {}
