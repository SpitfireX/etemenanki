use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    expression: TokenExpression,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenExpression {
    Token(Token),
    Sequence(Vec<TokenExpression>),
    Alternation(Vec<TokenExpression>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Any,
    Constrained {
        constraint: TokenConstraint,
        min: usize,
        max: usize,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pattern {
    identifier: Option<String>,
    searchstr: String,
    regex: bool,
}
