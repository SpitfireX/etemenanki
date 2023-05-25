use std::fs::File;
use std::io::Result;
use std::str;

use memmap2::Mmap;

fn main() -> Result<()> {
    let file = File::open("scripts/recipes4000/b764b867-cac4-4329-beda-9c021c5184d7.zigl")?;
    let mmap = unsafe { dbg!(Mmap::map(&file)?) };

    let header = mmap.as_ref().as_ptr() as *const Header;

    unsafe {
        let dh = header.as_ref().unwrap();
        println!("{}", str::from_utf8(&dh.uuid).unwrap());
        println!("{}", dh.used);

        let bom = ((mmap.as_ref().as_ptr().offset(160)) as *const [BOMEntry; 1]).as_ref().unwrap();
        println!("{}", str::from_utf8(&bom[0].name).unwrap());
    }

    Ok(())
}

#[repr(C, packed)]
struct Header {
    magic: [u8; 8],
    version: [u8;4],
    family: u8,
    class: u8,
    ctype: u8,
    lf: u8,
    uuid: [u8; 36],
    lfeot: [u8; 4],
    allocated: u8,
    used: u8,
    padding: [u8; 6],
    dim1: i64,
    dim2: i64,
    base1_uuid: [u8; 36],
    padding1: [u8; 4],
    base2_uuid: [u8; 36],
}

#[repr(C, packed)]
struct BOMEntry {
    family: u8,
    ctype: u8,
    mode: u8,
    name: [u8; 13],
    offset: i64,
    size: i64,
    param1: i64,
    param2: i64,
}
