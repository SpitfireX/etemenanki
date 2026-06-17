use etemenanki::Datastore;

pub mod parser;
mod ast;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    evaltree: ast::QueryNode,
    patterns: Vec<ast::Pattern>,
}

impl Query {
    fn new(evaltree: ast::QueryNode, patterns: Vec<ast::Pattern>) -> Self {
        Self {
            evaltree,
            patterns,
        }
    }

    pub fn print_debug(&self){
        println!("Evaltree:");
        println!("---------");
        println!("{:#?}\n", self.evaltree);

        println!("Patterns:");
        println!("---------");
        for (i, p) in self.patterns.iter().enumerate() {
            println!("[{i}] {:#?}", p);
        }
    }

    pub fn execute(&mut self, corpus: &Datastore) {
        let primary = corpus.layer_by_name("primary").unwrap();

        // patterns cannot be borrowed as mut uuuuuuhhhhhhhh
    }
}
