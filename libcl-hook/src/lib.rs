mod cl_helper_types;
use cl_helper_types::*;

use libcl_hook_macros::{hook, logged_hook};
use libc::{c_char, c_int};

#[logged_hook]
fn cl_new_corpus(registry_dir: *mut c_char, registry_name: *mut c_char) -> *mut Corpus {}

#[logged_hook]
fn cl_new_attribute(corpus: *mut Corpus, attribute_name: *mut c_char, attr_type: c_int) -> *mut Attribute {}

#[logged_hook]
fn cl_id2str(attribute: *mut Attribute, id: c_int) -> *mut c_char {}

#[logged_hook]
fn cl_cpos2struc2cpos(attribute: *mut Attribute, position: c_int, struc_start: *mut Out<c_int>, struc_end: *mut Out<c_int>) -> bool {}
