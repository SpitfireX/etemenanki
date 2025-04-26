use std::ffi::CString;
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemFn};

/// Macro that hooks a C function via its identifier and exports an `extern "C"` function of the same name.
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
            static ref #hooked_name: unsafe extern "C" fn(#fn_args) #fn_output = unsafe {
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


/// Handles transformation of identifiers (bindings) to expressions that are printable
/// using std::fmt facilities based on their type
fn get_format_expression(binding: Box<dyn quote::ToTokens>, ty: &syn::Type) -> impl quote::ToTokens {
    match ty {
        syn::Type::Ptr(tp) => {
            let ty = &tp.elem;

            match quote!(#ty).to_string().as_str() {
                "c_char" => quote!(CStr::from_ptr(#binding as *const i8)), // print strings instead of their address
                _ => quote!(#binding),
            }
        }
        _ => quote!(#binding),
    }
}


/// Returns the correct formatting placeholders for expressions produced by
/// `get_format_expression`
fn get_format_placeholder(_ty: &syn::Type) -> String {
    "{:?}".to_owned()
}


/// Macro that hooks a C function via its identifier and exports an `extern "C"` function of the same name.
/// Addetionally, the original function is called and its arguments and return value logged to stderr.
/// The decorated function needs to have the exact identifier and signature of the function to hook.
/// 
/// This macro uses the `hook` macro internally but automatically handles calling and forwarding of the
/// original function's return value.
/// 
/// The original function's return value can be accessed in the function body via an automatically
/// generated binding called `hooked_retval`. The hooked retval is automatically returned after the
/// function body.
#[proc_macro_attribute]
pub fn logged_hook(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;
    let fn_args = &input.sig.inputs;
    let fn_output = &input.sig.output;
    let fn_block = &input.block;

    // list of fn arguments to be printed
    let mut arg_placeholders = Vec::new();
    let mut arg_exprs = Vec::new();

    for arg in fn_args.iter() {
        if let FnArg::Typed(arg) = arg {
            arg_placeholders.push(get_format_placeholder(&arg.ty));
            arg_exprs.push(get_format_expression(arg.pat.clone(), &arg.ty));
        }
    }

    let mut format_str = "{}(".to_owned();
    format_str.push_str(&arg_placeholders.join(", "));
    format_str.push(')');

    // prefixed identifier for the hooked function
    let hooked_name = format_ident!("hooked_{}", fn_name.to_string());
    // collect the identifiers of all the function arudments
    let hooked_args: Vec<_> = fn_args.iter().filter_map(| arg | {
        if let FnArg::Typed(pat) = arg {
            Some(pat.pat.clone())
        } else {
            None
        }
    }).collect();

    // assemble print stateent based on function return type
    let print_stmt = if let syn::ReturnType::Type(_, ty) = fn_output {
        // if fn has return type, add output to format str
        format_str.push_str(" -> ");
        format_str.push_str(&get_format_placeholder(ty));

        // get correct argument expression for our hooked returval
        let retval = get_format_expression(Box::new(format_ident!("_hooked_retval")), ty);

        // assemble print statement with arrow and final _hooked_retval_ argument
        quote!{ println!(#format_str, stringify!(#fn_name), #(#arg_exprs),*, #retval); }
    } else {
        // assemble print statemint without return type
        quote!{ println!(#format_str, stringify!(#fn_name), #(#arg_exprs),*); }
    };

    // unsafe block containing the actual logging code
    // calls the hooked function first to get the retval
    // return the retval to the fn scope
    let logging_block = quote! {
        unsafe {
            let _hooked_retval = #hooked_name(#(#hooked_args),*);
            #print_stmt
            _hooked_retval
        }
    };

    // defer impl of actual hook to hook macro
    let implementation = quote! {
        #[hook]
        fn #fn_name(#fn_args) #fn_output {
            let hooked_retval = #logging_block;
            {
                #fn_block
            }
            hooked_retval
        }
    };

    TokenStream::from(implementation)
}
