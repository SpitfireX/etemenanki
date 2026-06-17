pub mod query;
#[cfg(test)]
mod tests;

use etemenanki::Datastore;

fn main() {
    let corpus = Datastore::open("../etemenanki/testdata/simpledickens").unwrap();
    let query = r#""hello" "world""#;
    query::parser::print_parsetree(&query);
    let mut query = query::parser::parse(&query).unwrap();
    query.print_debug();
    query.execute(&corpus);
}
