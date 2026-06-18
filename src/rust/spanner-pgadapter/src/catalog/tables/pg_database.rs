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

/// pg_database catalog view definition.
pub struct PgDatabase;

impl CatalogTable for PgDatabase {
    fn name(&self) -> &'static str {
        "pg_database"
    }

    fn table_expression(&self) -> &'static str {
        "pg_database as (
  select 0::bigint as oid,
         catalog_name as datname,
         0::bigint as datdba,
         6::bigint as encoding,
         'c' as datlocprovider,
         'C' as datcollate,
         'C' as datctype,
         false as datistemplate,
         true as datallowconn,
         -1::bigint as datconnlimit,
         0::bigint as datlastsysoid,
         0::bigint as datfrozenxid,
         0::bigint as datminmxid,
         0::bigint as dattablespace,
         null as daticulocale,
         null as daticurules,
         null as datcollversion,
         null as datacl
  /* Preferably, this should use information_schema.information_schema_catalog_name, but that does not exist on the emulator. */
  from (select distinct catalog_name from information_schema.schemata) catalogs
)"
    }
}
