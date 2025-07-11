use std::env;
use std::path::PathBuf;

fn main() {
    // Tell cargo to tell rustc to link the system libcl
    // shared library.
    println!("cargo:rustc-link-lib=cl");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // custom config
        .generate_cstr(true)
        .generate_comments(true)
        .generate_block(true)
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        // custom whitelist
        .allowlist_function("cl_.+")
        .allowlist_var("cl_errno")
        .allowlist_var("cl_regex_error")
        .allowlist_var("(ATT|CDA|CL|STRUC)_.+")
        .rustified_enum("ECorpusCharset")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
