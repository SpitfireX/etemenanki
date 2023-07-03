use std::{io::Result, str::FromStr};

use etemenanki::Datastore;
use uuid::Uuid;

fn main() -> Result<()> {
    // let file = File::open("../scripts/recipes4000/sattr_text_keywords.zigv")?;
    // let mmap = unsafe { dbg!(Mmap::map(&file)?) };

    // let container = Container::from_mmap(mmap).unwrap();
    // println!("{:?}", container);

    // let component = container.components.get("Partition").unwrap();
    // let vector = component.as_vector().unwrap();

    let datastore = Datastore::open("../scripts/recipes4000/").unwrap();

    println!("{:?}", datastore.layer_uuids());
    println!("{:?}", datastore.layer_names());

    let layer = datastore.layer_by_name("primary_layer").unwrap();
    println!("{:?}", layer.variable_names());
    let var = layer.variable_by_name("pattr_pos").unwrap().as_indexed_string();
    println!("{:?}", var);

    Ok(())
}
