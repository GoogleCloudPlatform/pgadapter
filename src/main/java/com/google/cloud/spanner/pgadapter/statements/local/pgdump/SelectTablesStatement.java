// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.statements.local.pgdump;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;

public class SelectTablesStatement implements LocalStatement {
  private static final Statement SELECT_TABLES_STATEMENT =
      Statement.of(
          "select 0 as tableoid, 0 as oid, table_name as relname, "
              + "null as relacl, null as rrelacl, null as initrelacl, null as initrrelacl, "
              + "'' as relkind, 0 as relnamespace, null as rolname, 0 as relchecks, "
              + "false as relhastriggers, true as relhasindex, false as relhasrules, false as relhasoids, "
              + "false as relrowsecurity, false as relforcerowsecurity, 0 as relfrozenxid, 0 as relminmxid, "
              + "0 as toid, 0 as tfrozenxid, 0 as tminmxid, 'p' as relpersistence, true as relispopulated, "
              + "null as relreplident, 0 as relpages, 'heap' as amname, 0 as foreignserver, "
              + "null as reloftype, null as owning_tab, null as owning_col, null as reltablespace, "
              + "null as reloptions, null as checkoption, null as toast_reloptions, false as is_identity_sequence, "
              + "false as changed_acl, false as ispartition, null as partbound "
              + "from information_schema.tables");

  @Override
  public String[] getSql() {
    return new String[] {
      "SELECT c.tableoid, c.oid, c.relname, (SELECT pg_catalog.array_agg(acl ORDER BY row_n) FROM (SELECT acl, row_n FROM pg_catalog.unnest(coalesce(c.relacl,pg_catalog.acldefault(CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::\"char\",c.relowner))) WITH ORDINALITY AS perm(acl,row_n) WHERE NOT EXISTS ( SELECT 1 FROM pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault(CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::\"char\",c.relowner))) AS init(init_acl) WHERE acl = init_acl)) as foo) AS relacl, (SELECT pg_catalog.array_agg(acl ORDER BY row_n) FROM (SELECT acl, row_n FROM pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault(CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::\"char\",c.relowner))) WITH ORDINALITY AS initp(acl,row_n) WHERE NOT EXISTS ( SELECT 1 FROM pg_catalog.unnest(coalesce(c.relacl,pg_catalog.acldefault(CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::\"char\",c.relowner))) AS permp(orig_acl) WHERE acl = orig_acl)) as foo) as rrelacl, NULL AS initrelacl, NULL as initrrelacl, c.relkind, c.relnamespace, (SELECT rolname FROM pg_catalog.pg_roles WHERE oid = c.relowner) AS rolname, c.relchecks, c.relhastriggers, c.relhasindex, c.relhasrules, 'f'::bool AS relhasoids, c.relrowsecurity, c.relforcerowsecurity, c.relfrozenxid, c.relminmxid, tc.oid AS toid, tc.relfrozenxid AS tfrozenxid, tc.relminmxid AS tminmxid, c.relpersistence, c.relispopulated, c.relreplident, c.relpages, am.amname, CASE WHEN c.relkind = 'f' THEN (SELECT ftserver FROM pg_catalog.pg_foreign_table WHERE ftrelid = c.oid) ELSE 0 END AS foreignserver, CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL END AS reloftype, d.refobjid AS owning_tab, d.refobjsubid AS owning_col, (SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions, CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, tc.reloptions AS toast_reloptions, c.relkind = 'S' AND EXISTS (SELECT 1 FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = c.oid AND objsubid = 0 AND refclassid = 'pg_class'::regclass AND deptype = 'i') AS is_identity_sequence, EXISTS (SELECT 1 FROM pg_attribute at LEFT JOIN pg_init_privs pip ON (c.oid = pip.objoid AND pip.classoid = 'pg_class'::regclass AND pip.objsubid = at.attnum)WHERE at.attrelid = c.oid AND ((SELECT pg_catalog.array_agg(acl ORDER BY row_n) FROM (SELECT acl, row_n FROM pg_catalog.unnest(coalesce(at.attacl,pg_catalog.acldefault('c',c.relowner))) WITH ORDINALITY AS perm(acl,row_n) WHERE NOT EXISTS ( SELECT 1 FROM pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault('c',c.relowner))) AS init(init_acl) WHERE acl = init_acl)) as foo) IS NOT NULL OR (SELECT pg_catalog.array_agg(acl ORDER BY row_n) FROM (SELECT acl, row_n FROM pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault('c',c.relowner))) WITH ORDINALITY AS initp(acl,row_n) WHERE NOT EXISTS ( SELECT 1 FROM pg_catalog.unnest(coalesce(at.attacl,pg_catalog.acldefault('c',c.relowner))) AS permp(orig_acl) WHERE acl = orig_acl)) as foo) IS NOT NULL OR NULL IS NOT NULL OR NULL IS NOT NULL))AS changed_acl, pg_get_partkeydef(c.oid) AS partkeydef, c.relispartition AS ispartition, pg_get_expr(c.relpartbound, c.oid) AS partbound FROM pg_class c LEFT JOIN pg_depend d ON (c.relkind = 'S' AND d.classid = c.tableoid AND d.objid = c.oid AND d.objsubid = 0 AND d.refclassid = c.tableoid AND d.deptype IN ('a', 'i')) LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid AND c.relkind <> 'p') LEFT JOIN pg_am am ON (c.relam = am.oid) LEFT JOIN pg_init_privs pip ON (c.oid = pip.objoid AND pip.classoid = 'pg_class'::regclass AND pip.objsubid = 0) WHERE c.relkind in ('r', 'S', 'v', 'c', 'm', 'f', 'p') ORDER BY c.oid",
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    return new QueryResult(
        backendConnection
            .getSpannerConnection()
            .getDatabaseClient()
            .singleUse()
            .executeQuery(SELECT_TABLES_STATEMENT));
  }
}
