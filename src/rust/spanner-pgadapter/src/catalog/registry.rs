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

use crate::catalog::table::CatalogTable;
use crate::catalog::tables::pg_database::PgDatabase;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Registry holding all supported PG catalog views and replacement names.
pub struct CatalogRegistry {
    tables: HashMap<&'static str, Box<dyn CatalogTable>>,
    replacements: HashMap<String, &'static str>,
}

impl CatalogRegistry {
    /// Returns the global thread-safe singleton instance of the registry.
    pub fn global() -> &'static Self {
        static INSTANCE: OnceLock<CatalogRegistry> = OnceLock::new();
        INSTANCE.get_or_init(Self::new)
    }

    fn new() -> Self {
        let mut tables: HashMap<&'static str, Box<dyn CatalogTable>> = HashMap::new();

        // Register catalog tables
        tables.insert("pg_database", Box::new(PgDatabase));

        let mut replacements = HashMap::new();
        for &name in tables.keys() {
            // Unqualified table name
            replacements.insert(name.to_string(), name);
            // Qualified table name
            replacements.insert(format!("pg_catalog.{}", name), name);
        }

        Self {
            tables,
            replacements,
        }
    }

    /// Looks up a catalog table expression by its normalized unqualified name.
    pub fn get_table(&self, name: &str) -> Option<&dyn CatalogTable> {
        self.tables.get(name).map(|t| t.as_ref())
    }

    /// Resolves a queried table name (case-insensitive) to its normalized target name if matched.
    pub fn resolve_replacement(&self, name: &str) -> Option<&'static str> {
        self.replacements.get(&name.to_lowercase()).copied()
    }
}
