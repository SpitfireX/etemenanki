#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    expression: TokenExpression,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenExpression {
    Token(Token),
    Seq(Vec<TokenExpression>),
    Dis(Vec<TokenExpression>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Any,
    Constrained {
        constraint: TokenConstraint,
        min: usize,
        max: usize,
    },
    None,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenConstraint {
    Atom {
        negated: bool,
        matchop: Match,
    },
    Con {
        negated: bool,
        left: Box<TokenConstraint>,
        right: Box<TokenConstraint>,
    },
    Dis {
        negated: bool,
        left: Box<TokenConstraint>,
        right: Box<TokenConstraint>,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    identifier: Option<String>,
    searchstr: String,
    regex: bool,
}
