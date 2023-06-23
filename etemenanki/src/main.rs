use std::fs::File;
use std::io::Result;

use memmap2::Mmap;

use etemenanki::container::Container;

fn main() -> Result<()> {
    let file = File::open("../scripts/recipes4000/sattr_text_keywords.zigv")?;
    let mmap = unsafe { dbg!(Mmap::map(&file)?) };

    let container = Container::from_mmap(mmap).unwrap();
    println!("{:?}", container);

    // let component = container.components.get("Partition").unwrap();
    // let vector = component.as_vector().unwrap();

    Ok(())
}
