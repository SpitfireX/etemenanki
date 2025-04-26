use libcl_hook_macros::hook;

use libc::{size_t, c_void};

#[hook]
pub unsafe extern "C" fn cl_malloc(size: size_t) -> *mut c_void {
    eprint!("Intercepted cl_malloc({}) -> ", size);
    let ptr = hooked_cl_malloc(size); // Call the real `malloc`
    eprintln!("0x{:x}", ptr.addr());
    ptr
}

#[hook]
pub unsafe extern "C" fn free(ptr: *mut c_void) {
    eprintln!("Intercepted free(0x{:x})", ptr.addr());
    hooked_free(ptr)
}
