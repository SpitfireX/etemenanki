use std::env;

use libcl_rs::*;

// simple example implementation of cwb-decode
fn main() -> AccessResult<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 3 {
        println!("Usage: cwb-decode <registry folder> <corpus name>");
        return Ok(());
    }

    let c = Corpus::new(&args[1], &args[2]).expect("Could not open corpus.");

    let pattrs: Vec<_> = c
        .list_p_attributes()
        .iter()
        .map(|name| c.get_p_attribute(name).unwrap())
        .collect();

    let sattrs: Vec<_> = c
        .list_s_attributes()
        .iter()
        .map(|name| (name.to_owned(), c.get_s_attribute(name).unwrap()))
        .collect();

    let clen = pattrs[0].max_cpos()?;

    for i in 0..clen {
        // print s attr start tags
        for (name, sattr) in sattrs.iter() {
            let bound = sattr.cpos2boundary(i)?;
            if bound & 2 == 2 {
                if let Ok(value) = sattr.cpos2struc2str(i) {
                    println!("<{} {}>", name, value.to_str().unwrap());
                } else {
                    println!("<{}>", name);
                }
            }
        }

        // print p attrs
        let strs: Vec<_> = pattrs
            .iter()
            .map(|attr| attr.cpos2str(i).unwrap().to_str().unwrap())
            .collect();
        println!("{}\t{}", i, strs.join("\t"));

        // print s attr end tags
        for (name, sattr) in sattrs.iter() {
            let bound = sattr.cpos2boundary(i)?;
            if bound & 4 == 4 {
                println!("</{}>", name);
            }
        }
    }

    Ok(())
}
