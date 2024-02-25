#![feature(iter_intersperse)]

use std::collections::HashSet;
use std::env;
use std::io::Result;

use etemenanki::components::FnvHash;
use etemenanki::Datastore;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let datastore = Datastore::open(&args[1]).expect("could not open datastore");

    let words = datastore["primary"]["word"]
        .as_indexed_string()
        .unwrap();

    for (i, w) in words.iter().enumerate() {
        println!("{}: {}", i, w);
    }

    let expected = "the";
    let the_id = words.lexicon().iter().position(|s| s == expected).unwrap();
    dbg!(the_id);
    dbg!(words.inverted_index().frequency(the_id).unwrap());
    let the: Vec<_> = words.inverted_index().positions(the_id).unwrap().collect();
    for tpos in the {
        let token = words.get_unchecked(tpos);
        assert!(token == expected, "token not expected");
    }


    // // let words = datastore["primary"]["word"]
    // //     .as_indexed_string()
    // //     .unwrap();

    // // let heads = datastore["primary"]["head-index"]
    // //     .as_pointer()
    // //     .unwrap();

    // // let relation = datastore["primary"]["relation"]
    // //     .as_indexed_string()
    // //     .unwrap();

    // // for cpos in 0..words.len() {
    // //     let word = words.get(cpos).unwrap();
    // //     if let Some(head) = heads.get(cpos) {
    // //         let relation = relation.get(cpos).unwrap();
    // //         let head_word = words.get(head).unwrap();
    // //         println!("{}: {} --> {}: {} ({})", cpos, word, head, head_word, relation);
    // //     } else {
    // //         println!("{}: {}", cpos, word);
    // //     }
    // // }

    // let strings = datastore["primary"]["token"]
    //     .as_indexed_string()
    //     .unwrap();

    // let pos = datastore["primary"]["pos"]
    //     .as_indexed_string()
    //     .unwrap();

    // let s = datastore["s"].as_segmentation().unwrap();
    // let text = datastore["text"].as_segmentation().unwrap();

    // let tests = [
    //     "Schinken",
    //     "Tortellini",
    //     "Hallo",
    //     "Cremefine",
    //     "Quäse",
    //     "Rahm",
    //     "Sahne",
    //     "Schlagsahne",
    //     "Tofu",
    // ];

    // let posses: HashSet<_> = pos.lexicon().all_starting_with("VVI").collect_strs();

    // for test in tests {
    //     let result = strings.index().get_first(test.fnv_hash());
    //     match result {
    //         Some(i) => {
    //             println!("{} in index at {}", test, i);

    //             let mut usage = HashSet::new();

    //             for &p in strings.inverted_index().get_postings(i as usize).unwrap().get_all() {
    //                 let nextpos = pos.get_unchecked(p + 1);
    //                 if posses.contains(nextpos) {
    //                     usage.insert(strings.get_unchecked(p + 1));

    //                     let sid = s.find_containing(p).unwrap();
    //                     let (start, end) = s.get_unchecked(sid);

    //                     let surface: String = strings
    //                         .get_range(start, end)
    //                         .map(|str| {
    //                             if str == test {
    //                                 format!("|{}|", str)
    //                             } else {
    //                                 str.to_owned()
    //                             }
    //                         })
    //                         .intersperse(" ".to_owned())
    //                         .collect();
    //                     println!("{}", surface);

    //                     let tid = text.find_containing(p).unwrap();
    //                     let title = &text["title"].as_plain_string().unwrap().get_unchecked(tid);
    //                     let author = &text["author"].as_indexed_string().unwrap().get_unchecked(tid);
    //                     let url = &text["url"].as_plain_string().unwrap().get_unchecked(tid);
    //                     let year = &text["year"]
    //                         .as_integer()
    //                         .unwrap()
    //                         .get_unchecked(tid);
    //                     let keywords = &text["keywords"].as_set().unwrap().get_unchecked(tid);
    //                     let ingredients = &text["ingredients"].as_set().unwrap().get_unchecked(tid);
    //                     println!(
    //                         "text {} with title \"{}\" from {} by {} at url {} with keywords {:?} using ingredients {:?}\n",
    //                         tid, title, year, author, url, keywords, ingredients
    //                     );
    //                 }
    //             }

    //             println!(
    //                 "{} => {}",
    //                 test,
    //                 usage.into_iter().intersperse(", ").collect::<String>()
    //             );
    //         }
    //         None => println!("{} not in index", test),
    //     }
    // }

    // let year = text.variable_by_name("year")
    //     .unwrap()
    //     .as_integer()
    //     .unwrap();

    // println!("\nthere are {} texts from 2016", year.get_all(2016).count());

    // let filename = "../scripts/recipes4000/s/s.zigl";
    // let file = File::open(filename).unwrap();
    // let mmap = unsafe { Mmap::map(&file) }.unwrap();
    // let container = Container::from_mmap(mmap, "word".to_owned()).unwrap();

    // let vector = container
    //     .components
    //     .get("RangeStream")
    //     .unwrap()
    //     .as_vector()
    //     .unwrap();

    // let mut cached = CachedVector::new(*vector);
    // let mut iter = cached.column_iter_range(0, 10, 15).unwrap();

    // while let Some(row) = iter.next() {
    //     println!("{:?}", row);
    // }
    // println!("loop done");
    // println!("{:?}", iter.next());

    Ok(())
}
