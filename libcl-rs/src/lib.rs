#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![feature(test)]

mod bindings {
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
