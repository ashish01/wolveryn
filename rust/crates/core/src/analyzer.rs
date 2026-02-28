/// A token produced by the analyzer, with its text and position in the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub text: String,
    pub position: u32,
}

/// Tokenize text into lowercase alphanumeric tokens.
///
/// Splits on any non-alphanumeric character (Unicode-aware).
/// Each token is lowercased. Returns tokens with their positional index.
pub fn tokenize(text: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut position: u32 = 0;
    let mut token_start = None;

    for (i, ch) in text.char_indices() {
        if ch.is_alphanumeric() {
            if token_start.is_none() {
                token_start = Some(i);
            }
        } else if let Some(start) = token_start {
            let word: String = text[start..i].chars().map(|c| c.to_lowercase().next().unwrap_or(c)).collect();
            if !word.is_empty() {
                tokens.push(Token { text: word, position });
                position += 1;
            }
            token_start = None;
        }
    }

    // Handle last token (no trailing delimiter)
    if let Some(start) = token_start {
        let word: String = text[start..].chars().map(|c| c.to_lowercase().next().unwrap_or(c)).collect();
        if !word.is_empty() {
            tokens.push(Token { text: word, position });
        }
    }

    tokens
}

/// Tokenize and return just the term strings (no positions).
pub fn tokenize_terms(text: &str) -> Vec<String> {
    tokenize(text).into_iter().map(|t| t.text).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tokenization() {
        let tokens = tokenize("Hello, World! Rust-based search");
        let terms: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world", "rust", "based", "search"]);
    }

    #[test]
    fn test_positions() {
        let tokens = tokenize("Hello, World!");
        assert_eq!(tokens[0].position, 0);
        assert_eq!(tokens[1].position, 1);
    }

    #[test]
    fn test_unicode() {
        let tokens = tokenize("café résumé naïve");
        let terms: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(terms, vec!["café", "résumé", "naïve"]);
    }

    #[test]
    fn test_empty_input() {
        assert!(tokenize("").is_empty());
        assert!(tokenize("   ").is_empty());
        assert!(tokenize("!@#$%").is_empty());
    }

    #[test]
    fn test_single_token() {
        let tokens = tokenize("rust");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "rust");
        assert_eq!(tokens[0].position, 0);
    }

    #[test]
    fn test_numbers_mixed() {
        let tokens = tokenize("version 3 of rust2024");
        let terms: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(terms, vec!["version", "3", "of", "rust2024"]);
    }

    #[test]
    fn test_uppercase() {
        let tokens = tokenize("HELLO WORLD");
        let terms: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world"]);
    }

    #[test]
    fn test_tokenize_terms() {
        let terms = tokenize_terms("Hello, World!");
        assert_eq!(terms, vec!["hello", "world"]);
    }
}
