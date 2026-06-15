use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryNode {
    Token(Token, Repetitions),
    Sequence(Vec<QueryNode>, Repetitions),
    Alternation(Vec<QueryNode>, Repetitions),
}

impl QueryNode {
    pub fn repetitions(&self) -> Repetitions {
        match self {
            QueryNode::Token(.., repetitions) => *repetitions,
            QueryNode::Sequence(.., repetitions) => *repetitions,
            QueryNode::Alternation(.., repetitions) => *repetitions,
        }
    }

    pub fn set_repetitions(&mut self, reps: Repetitions) {
        match self {
            QueryNode::Token(.., repetitions) => *repetitions = reps,
            QueryNode::Sequence(.., repetitions) => *repetitions = reps,
            QueryNode::Alternation(.., repetitions) => *repetitions = reps,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Repetitions {
    pub min: usize,
    pub max: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Any {
        magnitude: Option<usize>,
    },
    Constrained {
        constraint: TokenConstraint,
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

/// A search pattern, i.e. a search string over a variable.
/// E.g. "goose" or pos="NN"
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Pattern {
    pub varname: Option<String>,
    pub searchstr: String,
    pub is_regex: bool,
    pub magnitude: Option<usize>,
}
