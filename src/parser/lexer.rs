use std::str::Chars;

struct Lexer<'a> {
    ch: Chars<'a>,
}

impl<'a> Lexer<'a> {
    fn new(query: &'a str) -> Self {
        Self { ch: query.chars() }
    }

    fn next_token(&mut self) -> TokenType {
        match self.ch.next() {
            Some(c) => {
                if c == '*' {
                    TokenType::Asterisk
                } else if c.is_alphabetic() {
                    self.consume_word()
                } else if c.is_whitespace() {
                    TokenType::WhiteSpace
                } else {
                    TokenType::Unknown
                }
            }
            None => TokenType::Unknown,
        }
    }

    fn consume_word(&mut self) -> TokenType {
        for c in self.ch.by_ref() {
            if c.is_alphabetic() {
                continue;
            } else {
                break;
            }
        }

        TokenType::Word
    }
}

#[derive(Debug, PartialEq)]
enum TokenType {
    /// Represents '*' used for multiplication or selection all fields.
    Asterisk,

    /// A word that can be command or name (of tables/fields/variable).
    Word,

    WhiteSpace,

    Unknown,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_select() {
        let query = "SELECT * from table;".to_owned();
        let mut lex = Lexer::new(&query);

        assert_eq!(lex.next_token(), TokenType::Word);
        assert_eq!(lex.next_token(), TokenType::Asterisk);
        assert_eq!(lex.next_token(), TokenType::WhiteSpace);
        assert_eq!(lex.next_token(), TokenType::Word);
        assert_eq!(lex.next_token(), TokenType::Word);
    }
}
