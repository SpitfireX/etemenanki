#![feature(iter_intersperse)]

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

    let datastore = Datastore::open("../scripts/recipes_web/").unwrap();

    let strings = datastore["primary_layer"]["pattr_token"]
        .as_indexed_string()
        .unwrap();

    let pos = datastore["primary_layer"]["pattr_pos"]
        .as_indexed_string()
        .unwrap();

    let s = datastore["sattr_s"].as_segmentation().unwrap();
    let text = datastore["sattr_text"].as_segmentation().unwrap();

    // for (token, pos) in std::iter::zip(strings.iter(), pos.iter()){
    //     println!("{}\t{}", token, pos);
    // }

    // for (start, end) in s.iter() {
    //     println!("{}\t{}", start, end);
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
        "Tofu",
    ];

    let posses: HashSet<_> = pos.lexicon().all_starting_with("VVI").collect_strs();

    for test in tests {
        let result = strings.index().get_first(test.fnv_hash());
        match result {
            Some(i) => {
                println!("{} in index at {}", test, i);
                let positions: Vec<_> = strings.inverted_index().postings(i as usize).collect();

                let mut usage = HashSet::new();
                for p in positions {
                    let nextpos = pos.get_unchecked(p + 1);
                    if posses.contains(nextpos) {
                        usage.insert(strings.get_unchecked(p + 1));

                        let sid = s.find_containing(p).unwrap();
                        let (start, end) = s.get_unchecked(sid);

                        let surface: String = strings
                            .get_range(start, end)
                            .map(|str| {
                                if str == test {
                                    format!("|{}|", str)
                                } else {
                                    str.to_owned()
                                }
                            })
                            .intersperse(" ".to_owned())
                            .collect();
                        println!("{}", surface);

                        let tid = text.find_containing(p).unwrap();
                        let title = &text["sattr_text_title"].as_plain_string().unwrap()[tid];
                        let author = &text["sattr_text_author"].as_indexed_string().unwrap()[tid];
                        let url = &text["sattr_text_url"].as_plain_string().unwrap()[tid];
                        let year = &text["sattr_text_year"]
                            .as_integer()
                            .unwrap()
                            .get_unchecked(tid);
                        let keywords = &text["sattr_text_keywords"].as_set().unwrap().get_unchecked(tid);
                        let ingredients = &text["sattr_text_ingredients"].as_set().unwrap().get_unchecked(tid);
                        println!(
                            "text {} with title \"{}\" from {} by {} at url {} with keywords {:?} using ingredients {:?}\n",
                            tid, title, year, author, url, keywords, ingredients
                        );
                    }
                }

                println!(
                    "{} => {}",
                    test,
                    usage.into_iter().intersperse(", ").collect::<String>()
                );
            }
            None => println!("{} not in index", test),
        }
    }

    let all2016: Vec<_> = text.variable_by_name("sattr_text_year")
        .unwrap()
        .as_integer()
        .unwrap()
        .get_all(2016)
        .collect();

    println!("\nthere are {} texts from 2016", all2016.len());

    // let texts = datastore["sattr_text"].as_segmentation().unwrap();

    // let position = 200000;
    // let ti = texts.find_containing(position).unwrap();
    // let span = texts.get_unchecked(ti);

    // println!("position {} in text {}: {:?}", position, ti, span);
    // println!("text: {:?}", strings.get_range(span.0, span.1).intersperse(" ").collect::<String>());
    // println!("title: {}", &datastore["sattr_text"]["sattr_text_title"].as_plain_string().unwrap()[ti]);
    // println!("url: {}", &datastore["sattr_text"]["sattr_text_url"].as_plain_string().unwrap()[ti]);
    // println!("author: {}", &datastore["sattr_text"]["sattr_text_author"].as_indexed_string().unwrap().get(ti));

    // println!("{} in range {:?}", 666, texts.find_containing(666));

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
