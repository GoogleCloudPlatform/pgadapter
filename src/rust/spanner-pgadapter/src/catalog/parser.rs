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

use crate::catalog::registry::CatalogRegistry;
use google_cloud_spanner::connection::Dialect;
use google_cloud_spanner::connection::parser::SimpleParser;
use std::collections::HashSet;

/// Translates the query if it references PostgreSQL system catalog tables.
pub fn translate_query(sql: &str) -> String {
    // 1. Cheap prefix check to avoid expensive parsing for regular queries.
    let sql_lower = sql.to_lowercase();
    if !sql_lower.contains("pg_") && !sql_lower.contains("information_schema") {
        return sql.to_string();
    }

    let registry = CatalogRegistry::global();
    let mut parser = SimpleParser::new(sql, Dialect::PostgreSql);
    let mut translated_sql = String::new();
    let mut last_copied_pos = 0;
    let mut detected = HashSet::new();

    let sql_bytes = parser.sql();

    while parser.pos() < sql_bytes.len() {
        let start_pos = parser.pos();
        if let Some(kw) = parser.read_keyword() {
            let kw_lower = kw.to_lowercase();
            if matches!(
                kw_lower.as_str(),
                "from" | "join" | "insert" | "update" | "delete"
            ) {
                // Determine if we need to advance past helper keywords (like INTO, FROM, ONLY)
                let mut advanced = false;
                if kw_lower == "insert" {
                    advanced = parser.eat_keyword("into");
                } else if kw_lower == "delete" {
                    advanced = parser.eat_keyword("from");
                } else if kw_lower == "update" {
                    advanced = parser.eat_keyword("only");
                }

                // If insert/delete/update didn't match the expected helper keyword,
                // reset parser position and copy this word as regular SQL.
                if (kw_lower == "insert" || kw_lower == "delete" || kw_lower == "update")
                    && !advanced
                {
                    parser.set_pos(start_pos + kw.len());
                    continue;
                }

                parse_and_replace_table_references(
                    &mut parser,
                    registry,
                    &mut detected,
                    &mut translated_sql,
                    &mut last_copied_pos,
                );
            }
        } else {
            let c = sql_bytes[parser.pos()];
            if c == b'\'' || c == b'"' {
                parser.skip_quoted_string(c);
            } else {
                parser.set_pos(parser.pos() + 1);
            }
        }
    }

    // Append the remainder of the SQL query
    let raw_sql_str = std::str::from_utf8(sql_bytes).unwrap();
    if last_copied_pos < raw_sql_str.len() {
        translated_sql.push_str(&raw_sql_str[last_copied_pos..]);
    }

    if detected.is_empty() {
        return sql.to_string();
    }

    prepend_cte_expressions(sql, &translated_sql, detected, registry)
}

/// Parses a comma-separated list of table references and replaces catalog tables.
fn parse_and_replace_table_references(
    parser: &mut SimpleParser<'_>,
    registry: &CatalogRegistry,
    detected: &mut HashSet<&'static str>,
    translated_sql: &mut String,
    last_copied_pos: &mut usize,
) {
    let sql_bytes = parser.sql();
    loop {
        parser.skip_whitespace_and_comments();
        let start = parser.pos();
        if let Some(table_ref) = parser.eat_identifier() {
            let end = parser.pos();
            if let Some(normalized_name) = registry.resolve_replacement(&table_ref) {
                detected.insert(normalized_name);
                // Copy SQL up to this table reference start position
                let raw_sql_str = std::str::from_utf8(sql_bytes).unwrap();
                translated_sql.push_str(&raw_sql_str[*last_copied_pos..start]);
                // Append the unqualified normalized name
                translated_sql.push_str(normalized_name);
                *last_copied_pos = end;
            }
        }

        parser.skip_whitespace_and_comments();
        if !parser.eat_token(b',') {
            break;
        }
    }
}

/// Prepends the resolved CTE expressions representing system catalog schemas.
fn prepend_cte_expressions(
    sql: &str,
    translated_sql: &str,
    detected: HashSet<&'static str>,
    registry: &CatalogRegistry,
) -> String {
    // Resolve dependencies recursively
    let mut resolved_list = Vec::new();
    let mut resolved_set = HashSet::new();
    for name in detected {
        resolve_dependencies(name, registry, &mut resolved_list, &mut resolved_set);
    }

    // Build the prepended CTE clauses
    let mut cte_clauses = Vec::new();
    for name in resolved_list {
        if let Some(table) = registry.get_table(name) {
            cte_clauses.push(table.table_expression().to_string());
        }
    }
    let table_expressions_sql = cte_clauses.join(",\n");

    // Check if the original statement started with "with"
    let mut with_parser = SimpleParser::new(sql, Dialect::PostgreSql);
    let had_common_table_expressions = with_parser.eat_keyword("with");

    if had_common_table_expressions {
        let remainder = with_parser.remaining_sql().trim();
        format!("with {},\n{}", table_expressions_sql, remainder)
    } else {
        format!("with {}\n{}", table_expressions_sql, translated_sql)
    }
}

/// Recursively resolves dependencies of a catalog table in depth-first order.
fn resolve_dependencies(
    name: &'static str,
    registry: &CatalogRegistry,
    list: &mut Vec<&'static str>,
    set: &mut HashSet<&'static str>,
) {
    if set.contains(name) {
        return;
    }
    if let Some(table) = registry.get_table(name) {
        for &dep in table.dependencies() {
            resolve_dependencies(dep, registry, list, set);
        }
    }
    if set.insert(name) {
        list.push(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_query_no_catalog() {
        let sql = "SELECT * FROM Users WHERE id = 1";
        assert_eq!(translate_query(sql), sql);
    }

    #[test]
    fn test_translate_query_with_catalog_unqualified() {
        let sql = "SELECT * FROM pg_database WHERE datallowconn = true";
        let expected = "with pg_database as (\n  select 0::bigint as oid,\n         catalog_name as datname,\n         0::bigint as datdba,\n         6::bigint as encoding,\n         'c' as datlocprovider,\n         'C' as datcollate,\n         'C' as datctype,\n         false as datistemplate,\n         true as datallowconn,\n         -1::bigint as datconnlimit,\n         0::bigint as datlastsysoid,\n         0::bigint as datfrozenxid,\n         0::bigint as datminmxid,\n         0::bigint as dattablespace,\n         null as daticulocale,\n         null as daticurules,\n         null as datcollversion,\n         null as datacl\n  /* Preferably, this should use information_schema.information_schema_catalog_name, but that does not exist on the emulator. */\n  from (select distinct catalog_name from information_schema.schemata) catalogs\n)\nSELECT * FROM pg_database WHERE datallowconn = true";
        assert_eq!(translate_query(sql), expected);
    }

    #[test]
    fn test_translate_query_with_catalog_qualified() {
        let sql = "SELECT * FROM pg_catalog.pg_database WHERE datallowconn = true";
        let expected = "with pg_database as (\n  select 0::bigint as oid,\n         catalog_name as datname,\n         0::bigint as datdba,\n         6::bigint as encoding,\n         'c' as datlocprovider,\n         'C' as datcollate,\n         'C' as datctype,\n         false as datistemplate,\n         true as datallowconn,\n         -1::bigint as datconnlimit,\n         0::bigint as datlastsysoid,\n         0::bigint as datfrozenxid,\n         0::bigint as datminmxid,\n         0::bigint as dattablespace,\n         null as daticulocale,\n         null as daticurules,\n         null as datcollversion,\n         null as datacl\n  /* Preferably, this should use information_schema.information_schema_catalog_name, but that does not exist on the emulator. */\n  from (select distinct catalog_name from information_schema.schemata) catalogs\n)\nSELECT * FROM pg_database WHERE datallowconn = true";
        assert_eq!(translate_query(sql), expected);
    }
}
