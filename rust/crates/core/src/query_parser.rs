/// Recursive descent query parser.
///
/// Grammar:
///   query    → or_expr
///   or_expr  → and_expr (OR and_expr)*
///   and_expr → unary (AND? unary)*        // implicit AND between terms
///   unary    → NOT primary | primary
///   primary  → WORD | LPAREN query RPAREN

/// Query AST node.
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    Term(String),
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
}

/// Token types for the lexer.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    And,
    Or,
    Not,
    LParen,
    RParen,
}

/// Tokenize a query string into tokens.
fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '(' => {
                tokens.push(Token::LParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                chars.next();
            }
            _ if ch.is_alphanumeric() || ch == '_' => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        word.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                // Check for keywords (case-insensitive)
                match word.to_uppercase().as_str() {
                    "AND" => tokens.push(Token::And),
                    "OR" => tokens.push(Token::Or),
                    "NOT" => tokens.push(Token::Not),
                    _ => tokens.push(Token::Word(word.to_lowercase())),
                }
            }
            _ => {
                // Skip unknown characters
                chars.next();
            }
        }
    }
    tokens
}

/// Parser state.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        match self.next() {
            Some(ref tok) if tok == expected => Ok(()),
            Some(tok) => Err(format!("expected {:?}, got {:?}", expected, tok)),
            None => Err(format!("expected {:?}, got end of input", expected)),
        }
    }

    /// query → or_expr
    fn parse_query(&mut self) -> Result<Query, String> {
        let result = self.parse_or_expr()?;
        Ok(result)
    }

    /// or_expr → and_expr (OR and_expr)*
    fn parse_or_expr(&mut self) -> Result<Query, String> {
        let mut left = self.parse_and_expr()?;
        while self.peek() == Some(&Token::Or) {
            self.next(); // consume OR
            let right = self.parse_and_expr()?;
            left = match left {
                Query::Or(mut children) => {
                    children.push(right);
                    Query::Or(children)
                }
                _ => Query::Or(vec![left, right]),
            };
        }
        Ok(left)
    }

    /// and_expr → unary (AND? unary)*
    /// Implicit AND: two adjacent terms without an operator.
    fn parse_and_expr(&mut self) -> Result<Query, String> {
        let mut left = self.parse_unary()?;
        loop {
            // Explicit AND
            if self.peek() == Some(&Token::And) {
                self.next(); // consume AND
                let right = self.parse_unary()?;
                left = match left {
                    Query::And(mut children) => {
                        children.push(right);
                        Query::And(children)
                    }
                    _ => Query::And(vec![left, right]),
                };
                continue;
            }
            // Implicit AND: next token is a word, NOT, or LPAREN
            match self.peek() {
                Some(Token::Word(_)) | Some(Token::Not) | Some(Token::LParen) => {
                    let right = self.parse_unary()?;
                    left = match left {
                        Query::And(mut children) => {
                            children.push(right);
                            Query::And(children)
                        }
                        _ => Query::And(vec![left, right]),
                    };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    /// unary → NOT primary | primary
    fn parse_unary(&mut self) -> Result<Query, String> {
        if self.peek() == Some(&Token::Not) {
            self.next(); // consume NOT
            let expr = self.parse_primary()?;
            Ok(Query::Not(Box::new(expr)))
        } else {
            self.parse_primary()
        }
    }

    /// primary → WORD | LPAREN query RPAREN
    fn parse_primary(&mut self) -> Result<Query, String> {
        match self.peek() {
            Some(Token::Word(_)) => {
                if let Some(Token::Word(w)) = self.next() {
                    Ok(Query::Term(w))
                } else {
                    unreachable!()
                }
            }
            Some(Token::LParen) => {
                self.next(); // consume (
                let expr = self.parse_query()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(tok) => Err(format!("unexpected token: {:?}", tok)),
            None => Err("unexpected end of input".to_string()),
        }
    }
}

/// Parse a query string into a Query AST.
pub fn parse(input: &str) -> Result<Query, String> {
    let tokens = tokenize(input);
    if tokens.is_empty() {
        return Err("empty query".to_string());
    }
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query()?;
    if parser.pos < parser.tokens.len() {
        return Err(format!(
            "unexpected token at position {}: {:?}",
            parser.pos,
            parser.tokens[parser.pos]
        ));
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_term() {
        assert_eq!(parse("rust").unwrap(), Query::Term("rust".into()));
    }

    #[test]
    fn test_implicit_and() {
        assert_eq!(
            parse("rust programming").unwrap(),
            Query::And(vec![
                Query::Term("rust".into()),
                Query::Term("programming".into()),
            ])
        );
    }

    #[test]
    fn test_explicit_and() {
        assert_eq!(
            parse("rust AND programming").unwrap(),
            Query::And(vec![
                Query::Term("rust".into()),
                Query::Term("programming".into()),
            ])
        );
    }

    #[test]
    fn test_or() {
        assert_eq!(
            parse("rust OR python").unwrap(),
            Query::Or(vec![
                Query::Term("rust".into()),
                Query::Term("python".into()),
            ])
        );
    }

    #[test]
    fn test_not() {
        assert_eq!(
            parse("NOT java").unwrap(),
            Query::Not(Box::new(Query::Term("java".into())))
        );
    }

    #[test]
    fn test_complex_grouping() {
        assert_eq!(
            parse("(rust OR python) AND web").unwrap(),
            Query::And(vec![
                Query::Or(vec![
                    Query::Term("rust".into()),
                    Query::Term("python".into()),
                ]),
                Query::Term("web".into()),
            ])
        );
    }

    #[test]
    fn test_three_terms_implicit_and() {
        assert_eq!(
            parse("rust fast search").unwrap(),
            Query::And(vec![
                Query::Term("rust".into()),
                Query::Term("fast".into()),
                Query::Term("search".into()),
            ])
        );
    }

    #[test]
    fn test_mixed_and_or() {
        // "a OR b c" → OR(a, AND(b, c)) because AND binds tighter
        assert_eq!(
            parse("a OR b c").unwrap(),
            Query::Or(vec![
                Query::Term("a".into()),
                Query::And(vec![
                    Query::Term("b".into()),
                    Query::Term("c".into()),
                ]),
            ])
        );
    }

    #[test]
    fn test_empty_query() {
        assert!(parse("").is_err());
    }

    #[test]
    fn test_mismatched_parens() {
        assert!(parse("(rust").is_err());
    }

    #[test]
    fn test_case_insensitive_keywords() {
        assert_eq!(
            parse("rust and python").unwrap(),
            Query::And(vec![
                Query::Term("rust".into()),
                Query::Term("python".into()),
            ])
        );
        assert_eq!(
            parse("rust or python").unwrap(),
            Query::Or(vec![
                Query::Term("rust".into()),
                Query::Term("python".into()),
            ])
        );
    }

    #[test]
    fn test_nested_groups() {
        assert_eq!(
            parse("(a OR b) AND (c OR d)").unwrap(),
            Query::And(vec![
                Query::Or(vec![
                    Query::Term("a".into()),
                    Query::Term("b".into()),
                ]),
                Query::Or(vec![
                    Query::Term("c".into()),
                    Query::Term("d".into()),
                ]),
            ])
        );
    }

    #[test]
    fn test_not_with_and() {
        assert_eq!(
            parse("rust NOT java").unwrap(),
            Query::And(vec![
                Query::Term("rust".into()),
                Query::Not(Box::new(Query::Term("java".into()))),
            ])
        );
    }

    #[test]
    fn test_uppercase_terms_lowered() {
        assert_eq!(
            parse("HELLO WORLD").unwrap(),
            Query::And(vec![
                Query::Term("hello".into()),
                Query::Term("world".into()),
            ])
        );
    }
}
