use std::io::Result;

use etemenanki::Datastore;

fn main() -> Result<()> {
    // let file = File::open("../scripts/recipes4000/sattr_text_keywords.zigv")?;
    // let mmap = unsafe { dbg!(Mmap::map(&file)?) };

    // let container = Container::from_mmap(mmap).unwrap();
    // println!("{:?}", container);

    // let component = container.components.get("Partition").unwrap();
    // let vector = component.as_vector().unwrap();

    let datastore = Datastore::open("../scripts/recipes4000/").unwrap();

    let strings = datastore["primary_layer"]["pattr_token"]
        .as_indexed_string()
        .unwrap();

    for string in strings.lexicon().iter() {
        println!("{}", string);
    }

    // dbg!(strings);

    // println!("{:?}", datastore.layer_uuids());
    // println!("{:?}", datastore.layer_names());

    // let layer = &datastore["sattr_text"];
    // // let layer = &datastore["sattr_s"];
    // println!("{:?}", layer.variable_names());
    // let var = layer["sattr_text_url"].as_plain_string().unwrap();
    // // let var = layer["sattr_s_id"].as_plain_string().unwrap();
    // println!("{:?}", var);

    // let test: Vec<&str> = var.iter().collect();
    // println!("{:?}", test);

    Ok(())
}
