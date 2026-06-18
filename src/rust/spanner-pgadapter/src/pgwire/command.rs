// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Extracts the SQL command tag (e.g. SELECT, INSERT, UPDATE, DELETE) from the SQL string.
/// TODO: This method should be moved into the Rust Connection API parser.
pub fn parse_command_tag(sql: &str) -> String {
    let mut clean_sql = String::new();
    let mut characters = sql.chars().peekable();

    while let Some(character) = characters.next() {
        if character == '-' && characters.peek() == Some(&'-') {
            // Skip line comments
            while let Some(next_char) = characters.next() {
                if next_char == '\n' || next_char == '\r' {
                    break;
                }
            }
        } else if character == '/' && characters.peek() == Some(&'*') {
            // Skip block comments
            characters.next(); // consume '*'
            while let Some(next_char) = characters.next() {
                if next_char == '*' && characters.peek() == Some(&'/') {
                    characters.next(); // consume '/'
                    break;
                }
            }
        } else {
            clean_sql.push(character);
        }
    }

    if let Some(first_word) = clean_sql.trim().split_whitespace().next() {
        first_word.to_uppercase()
    } else {
        "SELECT".to_string()
    }
}

/// Formats a PG CommandComplete tag based on the raw command name (e.g. INSERT, SELECT) and row/affected count.
pub fn format_command_complete_tag(command: &str, count: usize) -> String {
    if command == "INSERT" {
        format!("INSERT 0 {}", count)
    } else {
        format!("{} {}", command, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_tag() {
        assert_eq!(parse_command_tag("SELECT 1"), "SELECT");
        assert_eq!(parse_command_tag("  insert into foo values (1)"), "INSERT");
        assert_eq!(
            parse_command_tag("-- comment\nUPDATE bar SET x = 1"),
            "UPDATE"
        );
        assert_eq!(
            parse_command_tag("/* block comment */delete from baz"),
            "DELETE"
        );
        assert_eq!(parse_command_tag(""), "SELECT");
    }

    #[test]
    fn test_format_command_complete_tag() {
        assert_eq!(format_command_complete_tag("SELECT", 5), "SELECT 5");
        assert_eq!(format_command_complete_tag("INSERT", 12), "INSERT 0 12");
        assert_eq!(format_command_complete_tag("UPDATE", 0), "UPDATE 0");
        assert_eq!(format_command_complete_tag("DELETE", 3), "DELETE 3");
    }
}
