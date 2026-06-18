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

use crate::pgwire::protocol::Decode;

pub(crate) const TERMINATE_IDENTIFIER: u8 = b'X';

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Terminate;

impl<'a> Decode<'a> for Terminate {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        // Terminate message has no fields, length is 4 (only the length field itself)
        // Source payload buffer is already stripped of the length, so it should be empty.
        if !source.is_empty() {
            return Err(anyhow::anyhow!(
                "terminate message should not contain payload"
            ));
        }
        Ok(Terminate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminate_decode() {
        let mut source = &[][..];
        let terminate = Terminate::decode(&mut source).expect("failed to decode Terminate");
        assert_eq!(terminate, Terminate);
    }

    #[test]
    fn test_terminate_decode_with_payload() {
        let payload = b"extra_bytes";
        let mut source = &payload[..];
        let result = Terminate::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "terminate message should not contain payload"
        );
    }
}
