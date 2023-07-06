use std::{io::Result, str::FromStr};

use etemenanki::Datastore;
use ziggurat_varint as varint;

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

    let layer = &datastore["sattr_text"];
    // let layer = &datastore["sattr_s"];
    println!("{:?}", layer.variable_names());
    let var = layer["sattr_text_url"].as_plain_string().unwrap();
    // let var = layer["sattr_s_id"].as_plain_string().unwrap();
    println!("{:?}", var);
    
    
    for i in 0..var.len() {
        println!("String #{}: {}", i+1, &var[i]);
    }

    // let numbers = [1i64, -10, 1024, 1882778, -2389, -13379001, 0, -64, 63, -8192, 8191, -1048576, 1048575, -134217728, 134217727, -17179869184, 17179869183, -2199023255552, 2199023255551, -281474976710656, 281474976710655, -36028797018963968, 36028797018963967, -9223372036854775808, 9223372036854775807];
    // println!("{:?}", &numbers);

    // let bytes = varint::encode_block(&numbers);
    // println!("{:02x?}", &bytes);

    // let decoded = varint::decode_block(&bytes);
    // println!("{:?}", &decoded);

    Ok(())
}
