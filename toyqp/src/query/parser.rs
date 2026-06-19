use crate::query::{Query, ast};

use std::collections::VecDeque;

use anyhow::{Result, anyhow};
use indexmap::IndexSet;
use pest::{Parser, iterators::Pair};
use pest_ascii_tree::print_ascii_tree;


#[derive(pest_derive::Parser)]
#[grammar = "toy.pest"]
struct ToyParser;

struct ParseState {
    patterns: IndexSet<ast::Pattern>,
}

impl ParseState {
    pub fn new() -> Self {
        Self {
            patterns: IndexSet::new(),
        }
    }
}

fn parse_query(ps: &mut ParseState, input: &str) -> Result<ast::QueryNode> {
    let mut pairs = ToyParser::parse(Rule::query, input)?;
    let query = pairs.next().unwrap();
    let root_expr = query.into_inner().next().unwrap();

    Ok(parse_expression(ps, root_expr)?)
}

fn parse_expression(ps: &mut ParseState, expr: Pair<Rule>) -> Result<ast::QueryNode> {
    if expr.as_rule() != Rule::expression {
        return Err(anyhow!("Expected expression, got: {:?}", expr.as_rule()));
    }

    let default_reps = ast::Repetitions{min: 1, max: Some(1)};
    let mut nodes: Vec<ast::QueryNode> = Vec::new();
    let mut splits = Vec::new();

    for pair in expr.into_inner() {
        match pair.as_rule() {
            Rule::expression => {
                let inner = parse_expression(ps, pair)?;
                nodes.push(inner);
            }

            Rule::token => {
                let inner = parse_token(ps, pair)?;
                nodes.push(ast::QueryNode::Token(inner, default_reps));
            }

            Rule::quantifier => {
                let reps = parse_quantifier(ps, pair)?;
                nodes.last_mut().unwrap().set_repetitions(reps);
            }

            Rule::disjunc => splits.push(nodes.len()),

            _ => unreachable!("Expression got impossible inner rule: {:?}", pair.as_rule()),
        }
    }

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
                ast::QueryNode::Sequence(vec, default_reps)
            }
        }).collect();

        Ok(ast::QueryNode::Alternation(seqs, default_reps))
    } else {
        Ok(ast::QueryNode::Sequence(nodes, default_reps))
    }
}

fn parse_token(ps: &mut ParseState, token: Pair<Rule>) -> Result<ast::Token> {
    if token.as_rule() != Rule::token {
        return Err(anyhow!("Expected token, got: {:?}", token.as_rule()));
    }

    let mut constraint = None;

    for pair in token.into_inner() {
        match pair.as_rule() {
            Rule::atom => constraint = Some(parse_atom(ps, pair)?),
            Rule::constraint => constraint = Some(parse_constraint(ps, pair, false)?),
            _ => unreachable!("Token got impossible inner rule: {:?}", pair.as_rule()),
        }
    }

    if let Some(c) = constraint {
        Ok(ast::Token::Constrained {
            constraint: c,
            magnitude: None,
        })
    } else {
        Ok(ast::Token::Any {
            magnitude: None,
        })
    }
}

fn parse_atom(ps: &mut ParseState, atom: Pair<Rule>) -> Result<ast::ConstraintNode> {
    if atom.as_rule() != Rule::atom {
        return Err(anyhow!("Expected atom, got: {:?}", atom.as_rule()));
    }

    let mut negated = false;
    let mut is_regex = false;
    let mut varname = None;
    let mut str_rule = None;

    for pair in atom.into_inner() {
        match pair.as_rule() {
            Rule::neg => negated = true,
            Rule::regex => {
                str_rule = Some(pair.as_str().to_owned());
                is_regex = true;
            }
            Rule::str => str_rule = Some(pair.as_str().to_owned()),
            Rule::ident => varname = Some(pair.as_str().to_owned()),
            _ => unreachable!("Atom got impossible inner rule: {:?}", pair.as_rule()),
        }
    }

    let searchstr = {
        let str_rule = str_rule.unwrap();
        let content = &str_rule[1..str_rule.len()-1];
        let mut s = String::new();

        // add anchors to regex if not present, otherwise we'd match subtokens
        if is_regex && !content.starts_with('^') { s.push('^'); }
        s.push_str(content);
        if is_regex && !content.ends_with('$') { s.push('$'); }

        s
    };

    let pattern = ast::Pattern {
        varname,
        is_regex,
        searchstr,
        magnitude: None,
        tids: None,
    };
    // intern the pattern in the global pattern set
    let (i, _) = ps.patterns.insert_full(pattern);

    Ok(ast::ConstraintNode::Pattern {
        negated,
        pattern: i,
    })
}

fn parse_quantifier(_ps: &mut ParseState, quantifier: Pair<Rule>) -> Result<ast::Repetitions> {
    if quantifier.as_rule() != Rule::quantifier {
        return Err(anyhow!(
            "Expected constraint, got: {:?}",
            quantifier.as_rule()
        ));
    }

    let q = quantifier.as_str();

    if q.chars().count() == 1 {
        match q.chars().nth(0).unwrap() {
            '?' => Ok(ast::Repetitions{ min: 0, max: Some(1)}),
            '+' => Ok(ast::Repetitions{ min: 1, max: None}),
            '*' => Ok(ast::Repetitions{ min: 0, max: None}),
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
        
        Ok(ast::Repetitions{min, max})
    }
}

fn parse_constraint(ps: &mut ParseState, constraint: Pair<Rule>, negated: bool) -> Result<ast::ConstraintNode> {
    if constraint.as_rule() != Rule::constraint {
        return Err(anyhow!(
            "Expected constraint, got: {:?}",
            constraint.as_rule()
        ));
    }

    let mut negate_subcnstr = false;    // flag for negating the following (sub)constraint, as parsing is handled here
    let mut subcnstrs = Vec::new();     // flat list of subconstraints
    let mut ops = Vec::new();           // list of boolean ops between subconstraints

    for pair in constraint.into_inner() {
        match pair.as_rule() {
            Rule::boolop => ops.push(pair.as_str().chars().nth(0)),
            Rule::atom => subcnstrs.push(parse_atom(ps, pair)?),
            Rule::neg => negate_subcnstr = true,
            Rule::constraint => {
                subcnstrs.push(parse_constraint(ps, pair, negate_subcnstr)?);
                negate_subcnstr = false; // reset negation, since only constraints can be negated in this top level rule
            }
            _ => unreachable!("Constraint got impossible inner rule: {:?}", pair.as_rule()),
        }
    }

    if subcnstrs.len() == 1 {
        // if there is only one subconstraint we don't need to build a constraint tree
        Ok(subcnstrs.remove(0))
    } else {
        // there must be an operator between every two contsraints (no implicit or dangling ops in grammar)
        assert!(ops.len() == subcnstrs.len()-1, "Wrong number of bool ops in constraint list");

        // we build the constraint tree from bottom up, last parsed subconstraint is right child of first node
        let mut right = subcnstrs.pop().unwrap();

        // there will be one node per operator in the input
        for op in ops.iter().rev() {
            let left = Box::new(subcnstrs.pop().unwrap());
            let bright = Box::new(right);
            let mut node = match op.unwrap() {
                '&' => ast::ConstraintNode::And { negated, left, right: bright },
                '|' => ast::ConstraintNode::Or { negated, left, right: bright },
                _ => unreachable!("Constraint got impossible boolop: {:?}", op),
            };

            // ad-hoc normalize the logical form so that only leaf nodes (patterns) are negated,
            // since eventually token sets are easier to invert and reason about.
            // this uses the basic De Morgan's laws.
            if node.negated() {
                node = node.normalize();
            }

            right = node;
        }

        Ok(right)
    }
}

pub fn parse(input: &str) -> Result<crate::query::Query> {
    let mut ps = ParseState::new();
    let evaltree = parse_query(&mut ps, input)?;
    Ok(Query::new(evaltree, ps.patterns.into_iter().collect()))
}

pub fn print_parsetree(input: &str) {
    println!("Query:");
    println!("------");
    println!("{}\n", input);
    
    println!("Parsetree:");
    println!("----------");
    print_ascii_tree(ToyParser::parse(Rule::query, input));
}
