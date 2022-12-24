struct Lexer<'a> {
    query: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(query: &'a str) -> Self {
        Self {
            query: query.as_bytes(),
            pos: 0,
        }
    }

    fn next_token(&mut self) -> TokenType {
        let ch = self.query.get(self.pos);
        self.pos += 1;
        match ch {
            Some(&c) => {
                if c == b'*' {
                    TokenType::Asterisk
                } else if c.is_ascii_alphabetic() {
                    self.consume_word()
                } else if c.is_ascii_whitespace() {
                    TokenType::WhiteSpace
                } else {
                    TokenType::Unknown
                }
            }
            None => TokenType::Unknown,
        }
    }

    fn consume_word(&mut self) -> TokenType {
        while let Some(ch) = self.query.get(self.pos) {
            self.pos += 1;
            if ch.is_ascii_alphabetic() {
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
