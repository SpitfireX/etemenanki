use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryNode {
    Token(Token),
    Sequence(Vec<QueryNode>),
    Alternation(Vec<QueryNode>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Any {
        min: usize,
        max: Option<usize>,
        magnitude: Option<usize>,
    },
    Constrained {
        constraint: TokenConstraint,
        min: usize,
        max: Option<usize>,
        magnitude: Option<usize>,
    },
    None,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenConstraint {
    Pattern {
        negated: bool,
        matches: Rc<Pattern>,
    },
    And {
        negated: bool,
        left: Box<TokenConstraint>,
        right: Box<TokenConstraint>,
    },
    Or {
        negated: bool,
        left: Box<TokenConstraint>,
        right: Box<TokenConstraint>,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Pattern {
    pub identifier: Option<String>,
    pub searchstr: String,
    pub is_regex: bool,
}
