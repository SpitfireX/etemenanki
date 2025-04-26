use std::ffi::CString;
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, ItemFn};

/// Macro that hooks a C function via its identifier.
/// The decorated function needs to have the exact identifier and signature of the function to hook.
/// The original (shadowed) function can be accessed via an automatically generated global function
/// pointer called `hooked_<identifier>`.
#[proc_macro_attribute]
pub fn hook(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;
    let fn_args = &input.sig.inputs;
    let fn_output = &input.sig.output;
    let fn_block = &input.block;

    // prefixed identifier for the hooked function
    let hooked_name = format_ident!("hooked_{}", fn_name.to_string());
    let fn_name_lit = proc_macro2::Literal::c_string(&CString::new(fn_name.to_string()).unwrap());

    let expanded = quote! {

        // static function pointer to the original function
        // resolved via the linker at runtime using dlsym
        lazy_static::lazy_static! {
            static ref #hooked_name: extern "C" fn(#fn_args) #fn_output = unsafe {
                let ptr = libc::dlsym(libc::RTLD_NEXT, #fn_name_lit.as_ptr() as *const _);
                std::mem::transmute(ptr)
            };
        }

        // decorated fn
        // set no_mangle so the symbol is exported correctly
        #[unsafe(no_mangle)]
        pub extern "C" fn #fn_name(#fn_args) #fn_output {
            #fn_block
        }
    };

    TokenStream::from(expanded)
}
