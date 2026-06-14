#![feature(hash_set_entry)]

use std::collections::{HashSet, VecDeque};
use std::rc::Rc;

use anyhow::{Ok, Result, anyhow};
use pest::Parser;
use pest::iterators::{Pair, Pairs};
use pest_ascii_tree::print_ascii_tree;

pub mod ast;

#[derive(pest_derive::Parser)]
#[grammar = "toy.pest"]
pub struct ToyParser;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    evaltree: Option<ast::QueryNode>,
    patterns: HashSet<Rc<ast::Pattern>>,
}

impl Query {
    fn new() -> Self {
        Self {
            evaltree: None,
            patterns: HashSet::new(),
        }
    }

    pub fn parse(input: &str) -> Result<Self> {
        let mut query = Self::new();
        query.evaltree = Some(query.parse_query(input)?);
        Ok(query)
    }

    fn parse_query(&mut self, input: &str) -> Result<ast::QueryNode> {
        println!("query: {}\n", input);
        
        println!("parse tree:");
        print_ascii_tree(ToyParser::parse(Rule::query, input));

        let mut pairs = ToyParser::parse(Rule::query, input)?;
        let query = pairs.next().unwrap();
        let root_expr = query.into_inner().next().unwrap();

        Ok(self.parse_expression(root_expr)?)
    }

    fn parse_expression(&mut self, expr: Pair<Rule>) -> Result<ast::QueryNode> {
        if expr.as_rule() != Rule::expression {
            return Err(anyhow!("Expected expression, got: {:?}", expr.as_rule()));
        }

        let mut nodes: Vec<ast::QueryNode> = Vec::new();
        let mut splits = Vec::new();

        for pair in expr.into_inner() {
            match pair.as_rule() {
                Rule::expression => {
                    let inner = self.parse_expression(pair)?;
                    nodes.push(inner);
                }

                Rule::token => {
                    let inner = self.parse_token(pair)?;
                    nodes.push(ast::QueryNode::Token(inner));
                }

                Rule::disjunc => splits.push(nodes.len()),

                _ => unreachable!("Expression got impossible inner rule: {:?}", pair.as_rule()),
            }
        }

        // println!("expr elements:\n{:#?}", nodes);
        // println!("expr splits: {:?}", splits);

        if splits.len() > 0 {
            let mut subvecs = VecDeque::new();
            for i in splits.iter().rev() {
                let tail = nodes.split_off(*i);
                subvecs.push_front(tail);
            }
            subvecs.push_front(nodes);

            let seqs = subvecs.into_iter().map(|mut vec| {
                if vec.len() == 1 {
                    vec.pop().unwrap()
                } else {
                    ast::QueryNode::Sequence(vec)
                }
            }).collect();

            Ok(ast::QueryNode::Alternation(seqs))
        } else {
            Ok(ast::QueryNode::Sequence(nodes))
        }
    }

    fn parse_token(&mut self, token: Pair<Rule>) -> Result<ast::Token> {
        if token.as_rule() != Rule::token {
            return Err(anyhow!("Expected token, got: {:?}", token.as_rule()));
        }

        let mut constraint = None;
        let mut repetition = (1, Some(1));

        for pair in token.into_inner() {
            match pair.as_rule() {
                Rule::atom => constraint = Some(self.parse_atom(pair)?),
                Rule::constraint => constraint = Some(self.parse_constraint(pair)?),
                Rule::quantifier => repetition = self.parse_quantifier(pair)?,
                _ => unreachable!("Token got impossible inner rule: {:?}", pair.as_rule()),
            }
        }

        if let Some(c) = constraint {
            Ok(ast::Token::Constrained {
                constraint: c,
                min: repetition.0,
                max: repetition.1,
                magnitude: None,
            })
        } else {
            Ok(ast::Token::Any {
                min: repetition.0,
                max: repetition.1,
                magnitude: None,
            })
        }
    }

    fn parse_atom(&mut self, atom: Pair<Rule>) -> Result<ast::TokenConstraint> {
        if atom.as_rule() != Rule::atom {
            return Err(anyhow!("Expected atom, got: {:?}", atom.as_rule()));
        }

        let mut negated = false;
        let mut is_regex = false;
        let mut identifier = None;
        let mut searchstr = None;

        for pair in atom.into_inner() {
            match pair.as_rule() {
                Rule::neg => negated = true,
                Rule::regex => {
                    searchstr = Some(pair.as_str().to_owned());
                    is_regex = true;
                }
                Rule::str => searchstr = Some(pair.as_str().to_owned()),
                Rule::ident => identifier = Some(pair.as_str().to_owned()),
                _ => unreachable!("Atom got impossible inner rule: {:?}", pair.as_rule()),
            }
        }

        let pattern = ast::Pattern {
            identifier,
            searchstr: searchstr.unwrap(),
            is_regex,
        };
        // intern the pattern in the global pattern hashset
        let rc = self.patterns.get_or_insert(Rc::new(pattern));

        Ok(ast::TokenConstraint::Pattern {
            negated,
            matches: rc.clone(),
        })
    }

    fn parse_quantifier(&self, quantifier: Pair<Rule>) -> Result<(usize, Option<usize>)> {
        if quantifier.as_rule() != Rule::quantifier {
            return Err(anyhow!(
                "Expected constraint, got: {:?}",
                quantifier.as_rule()
            ));
        }

        let q = quantifier.as_str();

        if q.chars().count() == 1 {
            match q.chars().nth(0).unwrap() {
                '?' => Ok((0, Some(1))),
                '+' => Ok((1, None)),
                '*' => Ok((0, None)),
                _ => unreachable!("Invalid quantifier: {}", q),
            }
        } else {
            let mut min: usize = 0;
            let mut max: Option<usize> = None;
            
            for s in q.split(',') {
                if s.starts_with('{') {
                    if s.chars().count() > 1 {
                        min = s[1..].parse()?;
                    }
                } else if s.ends_with('}') {
                    if s.chars().count() > 1 {
                        max = Some(s[..s.chars().count() - 1].parse()?);
                    }
                } else {
                    unreachable!("Invalid quantifier: {}", q)
                }
            }
            
            Ok((min, max))
        }
    }

    fn parse_constraint(&mut self, constraint: Pair<Rule>) -> Result<ast::TokenConstraint> {
        if constraint.as_rule() != Rule::constraint {
            return Err(anyhow!(
                "Expected constraint, got: {:?}",
                constraint.as_rule()
            ));
        }

        let mut negated = false;
        let mut subcnstrs = Vec::new();
        let mut ops = Vec::new();

        for pair in constraint.into_inner() {
            match pair.as_rule() {
                Rule::boolop => ops.push((subcnstrs.len(), pair.as_str().chars().nth(0))),
                Rule::neg => negated = true,
                Rule::atom => subcnstrs.push(self.parse_atom(pair)?),
                Rule::constraint => subcnstrs.push(self.parse_constraint(pair)?),
                _ => unreachable!("Constraint got impossible inner rule: {:?}", pair.as_rule()),
            }
        }

        // println!("negated {}", negated);
        // println!("subcnstrs {:?}", &subcnstrs);
        // println!("ops {:?}", &ops);

        if subcnstrs.len() == 1 {
            Ok(subcnstrs.remove(0))
        } else {
            todo!("complex constraints");
        }
    }
}

fn main() {
    let q1 = r#""hello" "world" | "hi" [pos="NN"]{,2} | "yo" ("world" | "planet")"#;
    let r = Query::parse(&q1).unwrap();
    println!("ast:\n{:#?}", r);
}
