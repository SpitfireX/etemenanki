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
        println!("Query execution");
        println!("-------------------\n");

        let primary = corpus.layer_by_name("primary").unwrap();
        let default_varname = "word"; // todo: remove hardcoded value

        println!("resolving patterns...");
        // calculate magnitude of patterns
        for (i, pat) in self.patterns.iter_mut().enumerate() {
            let varname = pat.varname.as_deref().unwrap_or(default_varname);
            let var = primary.variable_by_name(varname).unwrap().as_indexed_string().unwrap();

            // gather list of type IDs for searchstring
            pat.tids = if pat.is_regex {
                var.lexicon()
                    .scan_all_matching_regex(&pat.searchstr)
                    .map(|i| i.collect())
                    .filter(|v: &Vec<usize>| v.len() > 0)
            } else {
                var.lex_id(&pat.searchstr)
                    .map(|id| vec![id])
            };

            if let Some(tids) = &pat.tids {
                let magnitude = var.inverted_index().combined_frequency(tids);
                pat.magnitude = Some(magnitude);
            } else {
                pat.magnitude = Some(0);
            }

            println!("[{i}] {:#?}\n", pat);
        }

        println!("resolving constraints...");
        

        // println!("pruning tree...");
        // self.evaltree.prune();
        // self.print_debug();
    }
}
