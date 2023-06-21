use std::fs::File;
use std::io::Result;
use std::str;

use memmap2::Mmap;

use etemenanki::Container;

fn main() -> Result<()> {
    let file = File::open("../scripts/recipes4000/primary_layer.zigl")?;
    let mmap = unsafe { dbg!(Mmap::map(&file)?) };

    // let header = mmap.as_ref().as_ptr() as *const Header;

    // unsafe {
    //     let dh = header.as_ref().unwrap();
    //     println!("{}", str::from_utf8(&dh.uuid).unwrap());
    //     println!("{}", dh.used);

    //     let bom = ((mmap.as_ref().as_ptr().offset(160)) as *const [BOMEntry; 1]).as_ref().unwrap();
    //     println!("{}", str::from_utf8(&bom[0].name).unwrap());
    // }

    // actual new code

    match Container::from_mmap(&mmap) {
        Ok(c) => {
            println!("{:?}", c);
        }
        Err(e) => println!("Error {}", e),
    }

    Ok(())
}

