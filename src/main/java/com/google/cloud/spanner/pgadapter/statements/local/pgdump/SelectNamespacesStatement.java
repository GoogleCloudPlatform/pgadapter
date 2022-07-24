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

public class SelectNamespacesStatement implements LocalStatement {
  private static final Statement SELECT_NAMESPACES_STATEMENT =
      Statement.of(
          "select 0 as tableoid, 0 as oid, schema_name as nspname, null as rolname, null as nspacl, null as rnspacl, null as initnspacl, null as initrnspacl "
              + "from information_schema.schemata");

  @Override
  public String[] getSql() {
    return new String[] {
      "SELECT n.tableoid, n.oid, n.nspname, "
          + "(SELECT rolname FROM pg_catalog.pg_roles WHERE oid = nspowner) AS rolname, "
          + "(SELECT pg_catalog.array_agg(acl ORDER BY row_n) FROM (SELECT acl, row_n FROM pg_catalog.unnest(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) WITH ORDINALITY AS perm(acl,row_n) WHERE NOT EXISTS ( SELECT 1 FROM pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault('n',n.nspowner))) AS init(init_acl) WHERE acl = init_acl)) as foo) as nspacl, "
          + "(SELECT pg_catalog.array_agg(acl ORDER BY row_n) FROM (SELECT acl, row_n FROM pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault('n',n.nspowner))) WITH ORDINALITY AS initp(acl,row_n) WHERE NOT EXISTS ( SELECT 1 FROM pg_catalog.unnest(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) AS permp(orig_acl) WHERE acl = orig_acl)) as foo) as rnspacl, "
          + "NULL as initnspacl, NULL as initrnspacl "
          + "FROM pg_namespace n "
          + "LEFT JOIN pg_init_privs pip ON (n.oid = pip.objoid AND pip.classoid = 'pg_namespace'::regclass AND pip.objsubid = 0) ",
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    return new QueryResult(
        backendConnection
            .getSpannerConnection()
            .getDatabaseClient()
            .singleUse()
            .executeQuery(SELECT_NAMESPACES_STATEMENT));
  }
}
