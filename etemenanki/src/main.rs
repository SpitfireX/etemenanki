use std::collections::HashSet;
use std::io::Result;

use etemenanki::components::FnvHash;
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

    let pos = datastore["primary_layer"]["pattr_pos"]
        .as_indexed_string()
        .unwrap();

    // for (token, pos) in std::iter::zip(strings.iter(), pos.iter()){
    //     println!("{}\t{}", token, pos);
    // }

    let tests = [
        "Schinken",
        "Tortellini",
        "Hallo",
        "Cremefine",
        "Quäse",
        "Rahm",
        "Sahne",
        "Schlagsahne",
    ];

    let verbs: HashSet<_> = pos.lexicon().all_starting_with("VVI").collect_strs();

    for test in tests {
        let result = strings.index().get_first(test.fnv_hash());
        match result {
            Some(i) => {
                let positions: Vec<_> = strings.inverted_index().postings(i as usize).collect();

                let mut usage = HashSet::new();
                for p in positions {
                    let nextpos = pos.get(p+1);
                    if verbs.contains(nextpos) {
                        usage.insert(strings.get(p+1));
                    }

                    let surface: Vec<_> = strings.get_range(p-6, p+7).collect();
                    println!("{}", surface.join(" "));
                }

                print!("{} => ", test);
                for v in usage {
                    print!("{}, ", v)
                }
                println!();
            }
            None => println!("{} not in index", test),
        }
    }

    let texts = datastore["sattr_text"].as_segmentation().unwrap();
    println!("text ranges: {:?}", texts.iter().collect::<Vec<_>>());

    // let matches: Vec<_> = pos.lexicon().all_starting_with("V").collect_strs();
    // println!("All tags starting with V: {:?}", matches);

    // let matches: Vec<_> = pos.lexicon().all_ending_with("N").collect_strs();
    // println!("All tags ending with N: {:?}", matches);

    // let matches: Vec<_> = pos.lexicon().all_containing("A").collect_strs();
    // println!("All tags containing A: {:?}", matches);

    // let bla: Vec<_> = pos.lexicon().get_all(&[1, 2, 3]).collect();
    // println!("strings: {:?}", bla);

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
