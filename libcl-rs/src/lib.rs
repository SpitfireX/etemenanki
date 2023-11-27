#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use std::ffi::{CString, CStr};

    use super::*;
    use libc::free;

    #[test]
    fn malloc() {
        unsafe {
            let test = "Hello malloc!";
            let len = test.as_bytes().len();
            let ptr = cl_malloc(len);
            ptr.copy_from(test.as_ptr() as *const std::os::raw::c_void, len);

            let s = std::str::from_utf8(std::slice::from_raw_parts(ptr as *const u8, len)).unwrap();
            assert!(s == test);

            free(ptr);
        }
    }

    #[test]
    fn string() {
        unsafe {
            let astr = cl_autostring_new("Hello\0".as_bytes().as_ptr() as *const i8, 6);
            cl_autostring_concat(astr, " World!\0".as_bytes().as_ptr() as *mut i8);
            assert!(cl_autostring_len(astr) == 12);
            cl_autostring_delete(astr);
        }
    }

    #[test]
    fn open() {
        unsafe {
            let path = CString::new("/home/timm/Projekte/Programmieren/KLUE/soupchef-cwb/cwb/registry").unwrap();
            let name = CString::new("chefkoch").unwrap();
            let c = cl_new_corpus(path.as_ptr() as *mut i8, name.as_ptr() as *mut i8);

            let attrs = cl_corpus_list_attributes(c, ATT_ALL as i32);
            for i in 0..cl_string_list_size(attrs) {
                let name = CStr::from_ptr(cl_string_list_get(attrs, i));
                println!("{:?}", name);
            }
            cl_delete_string_list(attrs);

            cl_delete_corpus(c);
        }
    }

    #[test]
    fn posdecode() {
        unsafe {
            let path = CString::new("/home/timm/Projekte/Programmieren/KLUE/soupchef-cwb/cwb/registry").unwrap();
            let name = CString::new("chefkoch").unwrap();
            let c = cl_new_corpus(path.as_ptr() as *mut i8, name.as_ptr() as *mut i8);

            let attr = cl_new_attribute(c, "word\0".as_ptr() as *const i8, ATT_POS as i32);
            assert!(!attr.is_null());
            assert!(cl_attribute_mother_corpus(attr) == c);

            let max = cl_max_cpos(attr);
            assert!(max > 0);

            for i in 0..max {
                let word = CStr::from_ptr(cl_cpos2str(attr, i));
                println!("{}", word.to_str().unwrap());
            }
        }
    }
}
