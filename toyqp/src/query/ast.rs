
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryNode {
    Token(Token, Repetitions),
    Sequence(Vec<QueryNode>, Repetitions),
    Alternation(Vec<QueryNode>, Repetitions),
    None,
}

/// Nodes of the query evaluation tree
/// 
/// This is a recursive tree of repetitions and alternations with `Token`s as its leaves
impl QueryNode {
    pub fn repetitions(&self) -> Repetitions {
        match self {
            Self::Token(.., repetitions) => *repetitions,
            Self::Sequence(.., repetitions) => *repetitions,
            Self::Alternation(.., repetitions) => *repetitions,
            Self::None => Repetitions { min: 0, max: Some(0) },
        }
    }

    pub fn set_repetitions(&mut self, reps: Repetitions) {
        match self {
            Self::Token(.., repetitions) => *repetitions = reps,
            Self::Sequence(.., repetitions) => *repetitions = reps,
            Self::Alternation(.., repetitions) => *repetitions = reps,
            Self::None => (), // nop
        }
    }

    /// This function recursively prunes the query node tee before execution.
    /// 
    /// All nodes that don't have any matches in the corpus get eliminated. This means:
    /// * Tokens with estimated magnitude of 0 are marked as dead
    /// * Sequences containing any dead node or with a maximum repetition of 0 are marked as dead
    /// * Alternations remove all dead nodes and are makred dead themselves if no alive branches remain or the maximum repitisions are 0
    pub fn prune(&mut self) {
        match self {
            Self::Token(token, repetitions) => {
                token.prune();
                if repetitions.max == Some(0) || matches!(token, Token::None) {
                    self.mark_dead();
                }
            },

            Self::Sequence(query_nodes, repetitions) => {
                if repetitions.max == Some(0) {
                    self.mark_dead();
                } else {
                    for node in query_nodes {
                        node.prune();

                        // if any of the nodes in the sequence becomes none,
                        // the sequence can no longer be matched
                        if matches!(node, QueryNode::None) {
                            self.mark_dead();
                            break;
                        }
                    }
                }
            }

            Self::Alternation(query_nodes, repetitions) => {
                if repetitions.max == Some(0) {
                    self.mark_dead();
                } else {
                    query_nodes.extract_if(.., |node| {
                        node.prune();
                        matches!(node, QueryNode::None)
                    }).for_each(drop); // to run the whole ExtractIf iterator


                    if query_nodes.len() == 1 {
                        let inner = query_nodes.pop().unwrap();
                        *self = inner;
                    } else if query_nodes.is_empty() {
                        self.mark_dead();
                    }
                }
            }

            Self::None => (), // nop
        }
    }

    pub fn mark_dead(&mut self) {
        *self = Self::None;
    }

    /// This function recursively traverses the whole query node tree and performs magnitude resolution
    /// for all `Token` leaf nodes.
    /// 
    /// This method needs to be called before `prune()` so that prune has any effect.
    /// 
    /// Todo: This implementation is a bit ugly right now since it wouldn't need to be recursive with
    /// better data structure design. In the future Tokens should be externed into a contiguous array
    /// so that this resolution step could be performed in the query module directly (without having
    /// to pass state via arguments);
    pub fn resolve_magnitude(&mut self, patterns: &[Pattern], max_magnitude: usize) {
        match self {
            Self::None => (),

            Self::Sequence(query_nodes, ..) |
            Self::Alternation(query_nodes, ..) => {
                for node in query_nodes {
                    node.resolve_magnitude(patterns, max_magnitude);
                }
            }

            Self::Token(token, ..) => token.resolve_magnitude(patterns, max_magnitude),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Repetitions {
    /// Minimum number of repetitions.
    /// This is either zero or n.
    pub min: usize,
    /// Maximum number of repetitions.
    /// This is None for unbounded repetition or
    /// Some(n) for a bounded repetition including zero.
    pub max: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Any {
        // This is always equal to the variable length, but we'll explicitly intern it for now
        magnitude: Option<usize>,
    },
    Constrained {
        constraint: ConstraintNode,
        magnitude: Option<usize>,
    },
    None,
}

impl Token {
    /// Marks the token as dead if it doesn't have any matches in the corpus.
    /// Internally this will change the token to `Token::None`.
    pub fn prune(&mut self) {
        if let Self::Constrained { constraint: _,  magnitude } = self {
            if magnitude.is_some_and(|m| m == 0) {
                *self = Self::None; // mark self as dead
            }
        }
    }

    /// See `QueryNode::resolve_magnitude()`
    fn resolve_magnitude(&mut self, patterns: &[Pattern], max_magnitude: usize) {
        match self {
            Self::None => (),

            Self::Any { magnitude } => *magnitude = Some(max_magnitude),

            Self::Constrained { constraint, magnitude } => 
                *magnitude = Some(constraint.estimate_magnitude(patterns, max_magnitude)),
        }
    }
}

/// Nodes of the token constraint tree
/// 
/// This is a binary tree of operators with `Pattern`s as its leaves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConstraintNode {
    Pattern {
        negated: bool,
        pattern: usize
    },
    And {
        negated: bool,
        left: Box<ConstraintNode>,
        right: Box<ConstraintNode>,
    },
    Or {
        negated: bool,
        left: Box<ConstraintNode>,
        right: Box<ConstraintNode>,
    },
}

impl ConstraintNode {
    pub fn negated(&self) -> bool {
        match self {
            Self::Pattern { negated, .. } => *negated,
            Self::And { negated, .. } => *negated,
            Self::Or { negated, .. } => *negated,
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

    /// Uses the magnitude of each subpattern to estimate the whole constraint tree's magnitude.
    /// 
    /// Estimation works the following way:
    /// * Patterns: m(p) = m(p); m(!p) = max - m(p)
    /// * And: m(and) = min(m(left), m(right)); m(!and) = max - m(and)
    /// * Or: m(or) = m(left) + m(right); m(!or) = max - m(or)
    /// 
    /// In general the value returned will be 0 <= m < max
    fn estimate_magnitude(&self, patterns: &[Pattern], max_magnitude: usize) -> usize {
        let magnitude = match self {
            Self::Pattern { negated, pattern } => {
                let magnitude = &patterns[*pattern].magnitude;
                
                if let Some(size) = magnitude {
                    if *negated {
                        return max_magnitude - *size;
                    } else {
                        return *size;
                    }
                }

                0
            }

            Self::And { negated, left, right } => {
                let lmag = left.estimate_magnitude(patterns, max_magnitude);
                let rmag = right.estimate_magnitude(patterns, max_magnitude);

                if *negated {
                    max_magnitude - lmag.min(rmag)
                } else {
                    lmag.min(rmag)
                }
            }

            Self::Or { negated, left, right } => {
                let lmag = left.estimate_magnitude(patterns, max_magnitude);
                let rmag = right.estimate_magnitude(patterns, max_magnitude);

                if *negated {
                    max_magnitude - (lmag + rmag)
                } else {
                    lmag + rmag
                }
            }
        };

        magnitude.clamp(0, max_magnitude)
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
    pub tids: Option<Vec<usize>>,
}
