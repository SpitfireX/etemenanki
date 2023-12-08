#![feature(iter_intersperse)]

use std::collections::HashSet;
use std::io::Result;
use std::env;

use etemenanki::components::FnvHash;
use etemenanki::Datastore;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let datastore = Datastore::open(&args[1]).expect("could not open datastore");

    let strings = datastore["primary"]["token"]
        .as_indexed_string()
        .unwrap();

    let pos = datastore["primary"]["pos"]
        .as_indexed_string()
        .unwrap();

    let s = datastore["s"].as_segmentation().unwrap();
    let text = datastore["text"].as_segmentation().unwrap();

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
                        let title = &text["title"].as_plain_string().unwrap()[tid];
                        let author = &text["author"].as_indexed_string().unwrap()[tid];
                        let url = &text["url"].as_plain_string().unwrap()[tid];
                        let year = &text["year"]
                            .as_integer()
                            .unwrap()
                            .get_unchecked(tid);
                        let keywords = &text["keywords"].as_set().unwrap().get_unchecked(tid);
                        let ingredients = &text["ingredients"].as_set().unwrap().get_unchecked(tid);
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

    let all2016: Vec<_> = text.variable_by_name("year")
        .unwrap()
        .as_integer()
        .unwrap()
        .get_all(2016)
        .collect();

    println!("\nthere are {} texts from 2016", all2016.len());

    Ok(())
}
