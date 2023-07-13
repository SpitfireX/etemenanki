use std::io::Result;

use etemenanki::Datastore;
use etemenanki::components::FnvHash;

fn main() -> Result<()> {
    // let file = File::open("../scripts/recipes4000/sattr_text_keywords.zigv")?;
    // let mmap = unsafe { dbg!(Mmap::map(&file)?) };

    // let container = Container::from_mmap(mmap).unwrap();
    // println!("{:?}", container);

    // let component = container.components.get("Partition").unwrap();
    // let vector = component.as_vector().unwrap();

    let datastore = Datastore::open("../scripts/recipes_web/").unwrap();

    let strings = datastore["primary_layer"]["pattr_token"]
        .as_indexed_string()
        .unwrap();

    let pos = datastore["primary_layer"]["pattr_pos"]
        .as_indexed_string()
        .unwrap();

    // for (token, pos) in std::iter::zip(strings.iter(), pos.iter()){
    //     println!("{}\t{}", token, pos);
    // }

    let tests = ["Schinken", "Tortellini", "Hallo", "Cremefine", "Quäse", "Rahm", "Sahne", "Schlagsahne"];

    for test in tests {
        let result = strings.index().get_first(test.fnv_hash());
        match result {
            Some(i) => {
                println!("{} in index at {}: {}", test, i, &strings.lexicon()[i as usize]);
                let freq = strings.inverted_index().frequency(i as usize);
                let positions: Vec<_> = strings.inverted_index().postings(i as usize).collect();
                println!("{} appears in the corpus {} times", test, freq);
                for p in positions {
                    print!("\t");
                    for s in strings.get_range(p-6, p+7) {
                        if s == test {
                            print!("|{}| ", s);
                        } else {
                            print!("{} ", s);
                        }
                    }
                    println!();
                }
            }
            None => println!("{} not in index", test),
        }
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
