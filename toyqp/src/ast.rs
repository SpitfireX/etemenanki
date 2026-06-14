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
        pattern: Rc<Pattern>,
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
    },
}

impl TokenConstraint {
    pub fn negated(&self) -> bool {
        match self {
            TokenConstraint::Pattern { negated, .. } => *negated,
            TokenConstraint::And { negated, .. } => *negated,
            TokenConstraint::Or { negated, .. } => *negated,
        }
    }

    /// Normalizes a given constraint using De Morgan's laws, so that only leaf nodes (patterns) are negated.
    pub fn normalize(self) -> Self {
        // no normalization necessary if non-negated inner node
        if !self.negated() && !matches!(self, Self::Pattern { .. }) {
            return self;
        }

        // otherwise do the transformation
        match self {
            // pattern -> !pattern
            Self::Pattern { negated, pattern } => Self::Pattern {
                negated: !negated,
                pattern,
            },

            // !(left & right) -> (!left | !right)
            Self::And {
                negated,
                left,
                right,
            } => Self::Or {
                negated: !negated,
                left: Box::new(left.normalize()),
                right: Box::new(right.normalize()),
            },

            // !(left | right) -> (!left & !right)
            Self::Or {
                negated,
                left,
                right,
            } => Self::And {
                negated: !negated,
                left: Box::new(left.normalize()),
                right: Box::new(right.normalize()),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Pattern {
    pub varname: Option<String>,
    pub searchstr: String,
    pub is_regex: bool,
    pub magnitude: Option<usize>,
}
