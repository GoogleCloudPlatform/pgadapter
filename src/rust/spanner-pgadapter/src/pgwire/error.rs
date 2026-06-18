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

use std::error::Error;
use std::fmt;

use google_cloud_spanner::Error as SpannerError;
use google_cloud_spanner::error::Code;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PGError {
    pub(crate) severity: String,
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) hint: Option<String>,
    pub(crate) detail: Option<String>,
}

impl PGError {
    pub(crate) fn new(severity: &str, code: &str, message: &str) -> Self {
        Self {
            severity: severity.to_string(),
            code: code.to_string(),
            message: message.to_string(),
            hint: None,
            detail: None,
        }
    }

    pub(crate) fn with_hint(mut self, hint: &str) -> Self {
        self.hint = Some(hint.to_string());
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_detail(mut self, detail: &str) -> Self {
        self.detail = Some(detail.to_string());
        self
    }
}

impl fmt::Display for PGError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.severity, self.message)
    }
}

impl Error for PGError {}

impl From<&anyhow::Error> for PGError {
    fn from(error: &anyhow::Error) -> Self {
        if let Some(spanner_err) = error.downcast_ref::<SpannerError>() {
            if let Some(status) = spanner_err.status() {
                let msg_lower = status.message.to_lowercase();

                // 1. Aborted transaction
                if status.code == Code::Aborted {
                    return PGError::new("ERROR", "40001", &status.message); // SerializationFailure
                }

                // 2. Undefined column
                if (status.code == Code::NotFound || status.code == Code::InvalidArgument)
                    && msg_lower.contains("column")
                    && msg_lower.contains("of relation")
                    && msg_lower.contains("does not exist")
                {
                    return PGError::new("ERROR", "42703", &status.message); // UndefinedColumn
                }

                // 3. Undefined table / relation
                if (status.code == Code::NotFound || status.code == Code::InvalidArgument)
                    && msg_lower.contains("relation")
                    && msg_lower.contains("does not exist")
                {
                    return PGError::new("ERROR", "42P01", &status.message); // UndefinedTable
                }

                // 4. Unique violation
                if status.code == Code::AlreadyExists
                    && (msg_lower.contains("already exists")
                        || msg_lower.contains("previously existing row")
                        || msg_lower.contains("unique index violation")
                        || msg_lower.contains("unique violation"))
                {
                    return PGError::new("ERROR", "23505", &status.message); // UniqueViolation
                }

                // 5. Foreign key violation
                if status.code == Code::FailedPrecondition
                    && (msg_lower.contains("foreign key")
                        && (msg_lower.contains("constraint violation")
                            || msg_lower.contains("violated on table")))
                {
                    return PGError::new("ERROR", "23503", &status.message); // ForeignKeyViolation
                }

                // 6. Feature not supported
                if (status.code == Code::FailedPrecondition || status.code == Code::InvalidArgument)
                    && msg_lower.contains("cannot drop table")
                    && msg_lower.contains("with indices")
                {
                    return PGError::new("ERROR", "0A000", &status.message) // FeatureNotSupported
                        .with_hint("Execute 'set spanner.support_drop_cascade=true' to enable dropping tables with indices");
                }

                if (status.code == Code::FailedPrecondition || status.code == Code::InvalidArgument)
                    && msg_lower.contains("only <restrict> behavior is supported")
                {
                    return PGError::new("ERROR", "0A000", &status.message) // FeatureNotSupported
                        .with_hint("Execute 'set spanner.support_drop_cascade=true' to enable 'drop {table|schema} cascade' statements.");
                }

                // 7. Timeout / Query cancelled
                if status.code == Code::DeadlineExceeded {
                    return PGError::new(
                        "ERROR",
                        "57014",
                        "canceling statement due to statement timeout",
                    ); // QueryCanceled
                }

                // Otherwise, map standard gRPC status code directly to default PG SQLState codes
                let code = match status.code {
                    Code::InvalidArgument => "22000",  // DataException
                    Code::PermissionDenied => "42501", // InsufficientPrivilege
                    Code::Unauthenticated => "28P01",  // InvalidPassword
                    Code::Unimplemented => "0A000",    // FeatureNotSupported
                    _ => "XX000",                      // Internal Error
                };
                return PGError::new("ERROR", code, &status.message);
            }
        }

        // Check if the anyhow error is already a PGError
        if let Some(pg_err) = error.downcast_ref::<PGError>() {
            return pg_err.clone();
        }

        // Generic error mapping
        PGError::new("ERROR", "XX000", &error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_spanner::error::{Code, Status};

    fn make_spanner_error(code: Code, message: &str) -> anyhow::Error {
        let status = Status::default().set_code(code).set_message(message);
        let err = SpannerError::service(status);
        anyhow::Error::new(err)
    }

    #[test]
    fn test_error_conversion_relation_not_found() {
        let err = make_spanner_error(Code::InvalidArgument, "relation \"foo\" does not exist");
        let pg_err = PGError::from(&err);
        assert_eq!(pg_err.code, "42P01");
        assert_eq!(pg_err.severity, "ERROR");
    }

    #[test]
    fn test_error_conversion_column_not_found() {
        let err = make_spanner_error(
            Code::NotFound,
            "column \"bar\" of relation \"foo\" does not exist",
        );
        let pg_err = PGError::from(&err);
        assert_eq!(pg_err.code, "42703");
    }

    #[test]
    fn test_error_conversion_unique_violation() {
        let err = make_spanner_error(Code::AlreadyExists, "Row [1] in table foo already exists");
        let pg_err = PGError::from(&err);
        assert_eq!(pg_err.code, "23505");
    }

    #[test]
    fn test_error_conversion_foreign_key_violation() {
        let err = make_spanner_error(
            Code::FailedPrecondition,
            "Foreign key constraint fk_bar is violated on table bar. Cannot find referenced values in foo",
        );
        let pg_err = PGError::from(&err);
        assert_eq!(pg_err.code, "23503");
    }

    #[test]
    fn test_error_conversion_deadline_exceeded() {
        let err = make_spanner_error(Code::DeadlineExceeded, "deadline exceeded");
        let pg_err = PGError::from(&err);
        assert_eq!(pg_err.code, "57014");
        assert_eq!(
            pg_err.message,
            "canceling statement due to statement timeout"
        );
    }

    #[test]
    fn test_error_conversion_generic_anyhow() {
        let err = anyhow::anyhow!("ordinary error");
        let pg_err = PGError::from(&err);
        assert_eq!(pg_err.code, "XX000");
        assert_eq!(pg_err.message, "ordinary error");
    }
}
