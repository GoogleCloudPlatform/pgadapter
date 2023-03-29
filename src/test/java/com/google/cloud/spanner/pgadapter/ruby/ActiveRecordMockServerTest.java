// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.ruby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@Category(RubyTest.class)
@RunWith(JUnit4.class)
public class ActiveRecordMockServerTest extends AbstractRubyMockServerTest {

  private static final class Singer {
    final String singerId;
    String firstName;
    String lastName;
    boolean active;
    Timestamp createdAt;
    Timestamp updatedAt;
    long version;

    Singer(
        String singerId,
        String firstName,
        String lastName,
        boolean active,
        Timestamp createdAt,
        Timestamp updatedAt,
        long version) {
      this.singerId = singerId;
      this.firstName = firstName;
      this.lastName = lastName;
      this.active = active;
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
      this.version = version;
    }
  }

  private static final class Album {
    final String albumId;
    String title;
    String singerId;

    Album(String albumId, String title, String singerId) {
      this.albumId = albumId;
      this.title = title;
      this.singerId = singerId;
    }
  }

  private static final String DIRECTORY_NAME = "./src/test/ruby/activerecord";

  @BeforeClass
  public static void installGems() throws Exception {
    createVirtualEnv(DIRECTORY_NAME);
  }

  @Test
  public void testDbMigrate() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_MIGRATIONS_TABLE,
            ResultSet.newBuilder().setMetadata(SELECT_RELNAME_METADATA).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_AR_INTERNAL_METADATA_TABLE,
            ResultSet.newBuilder().setMetadata(SELECT_RELNAME_METADATA).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_SCHEMA_MIGRATIONS_COLUMNS, SELECT_SCHEMA_MIGRATIONS_COLUMNS_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_AR_INTERNAL_METADATA_COLUMNS, SELECT_AR_INTERNAL_METADATA_COLUMNS_RESULTSET));
    // Add an empty migrations result.
    addMigrationResults();

    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_MIGRATIONS_TABLE_EXTENDED,
            ResultSet.newBuilder()
                .setMetadata(SELECT_RELNAME_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("schema_migrations").build())
                        .build())
                .build()));
    addInsertMigrationResult("1");
    addSelectArInternalMetadataResult("environment", 1L);
    addSelectTablesResult("ar_internal_metadata", "schema_migrations", "albums", "singers");
    addSelectTablesResultWithRelKinds(
        "'r','p'", "ar_internal_metadata", "schema_migrations", "albums", "singers");
    addInsertArInternalMetadataResult("environment", "default_env");
    addSelectSampleTablesResult();

    // For initializing the DDL executor.
    addDdlResponseToSpannerAdmin();
    // CREATE TABLE schema_migrations
    addDdlResponseToSpannerAdmin();
    // CREATE TABLE ar_internal_metadata
    addDdlResponseToSpannerAdmin();
    // The actual migration.
    addDdlResponseToSpannerAdmin();

    run(
        new String[] {"bundle", "exec", "rake", "db:migrate"},
        DIRECTORY_NAME,
        ImmutableMap.of("PGHOST", "localhost", "PGPORT", String.valueOf(pgServer.getLocalPort())));

    assertEquals(3, mockDatabaseAdmin.getRequests().size());
    UpdateDatabaseDdlRequest request =
        (UpdateDatabaseDdlRequest) mockDatabaseAdmin.getRequests().get(2);
    assertEquals(2, request.getStatementsCount());
    assertEquals(
        "CREATE TABLE \"singers\" ("
            + "\"singer_id\" character varying(36) NOT NULL PRIMARY KEY, "
            + "\"first_name\" character varying(100), "
            + "\"last_name\" character varying(200) NOT NULL, "
            + "\"full_name\" character varying GENERATED ALWAYS AS (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) STORED, "
            + "\"active\" boolean, "
            + "\"created_at\" timestamptz, "
            + "\"updated_at\" timestamptz, "
            + "\"lock_version\" integer NOT NULL)",
        request.getStatements(0));
    assertEquals(
        "CREATE TABLE \"albums\" ("
            + "\"album_id\" character varying(36) NOT NULL PRIMARY KEY, "
            + "\"title\" character varying, "
            + "\"marketing_budget\" decimal, "
            + "\"release_date\" date, "
            + "\"cover_picture\" bytea, "
            + "\"singer_id\" character varying(36), "
            + "\"created_at\" timestamptz, "
            + "\"updated_at\" timestamptz, "
            + "\"lock_version\" integer NOT NULL, "
            + "CONSTRAINT \"fk_rails_df791b93c8\"\n"
            + "FOREIGN KEY (\"singer_id\")\n"
            + "  REFERENCES \"singers\" (\"singer_id\")\n"
            + ")",
        request.getStatements(1));
  }

  @Test
  public void testRakeRun() throws Exception {
    addMigrationsTableResults();
    addSelectSampleTablesResult();
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.of("DELETE FROM \"albums\""), 0L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.of("DELETE FROM \"singers\""), 0L));

    Singer singer =
        new Singer(
            randomUuid(),
            "some-first-name",
            "some-last-name",
            true,
            Timestamp.now(),
            Timestamp.now(),
            1);
    addInsertSingerResult(singer, false);
    addSelectSingersResult(ImmutableList.of(singer));

    Album album = new Album(randomUuid(), "some-title", singer.singerId);
    addInsertAlbumResult(album, false);
    addSelectAlbumsOfSingerResult(
        singer.singerId,
        ImmutableList.of(
            new Album(randomUuid(), "some-title-1", singer.singerId),
            new Album(randomUuid(), "some-title-2", singer.singerId),
            new Album(randomUuid(), "some-title-3", singer.singerId)),
        true);

    singer.firstName = "Dave";
    singer.lastName = "Anderson";
    addUpdateSingerResult(singer, false);
    addSelectSingerResult(singer);

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT \"singers\".* FROM \"singers\" WHERE \"singers\".\"last_name\" LIKE 'A%'"),
            ResultSet.newBuilder()
                .setMetadata(createSelectSingersMetadata())
                .addRows(createSingerRecord(singer))
                .build()));

    run(
        new String[] {"bundle", "exec", "rake", "run"},
        DIRECTORY_NAME,
        ImmutableMap.of(
            "PGHOST",
            "localhost",
            "PGPORT",
            String.valueOf(pgServer.getLocalPort()),
            "RAILS_ENV",
            "development"));

    List<ExecuteSqlRequest> selectRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(getSelectSingerSql()))
            .collect(Collectors.toList());
    assertEquals(2, selectRequests.size());
    assertTrue(selectRequests.get(0).getTransaction().hasSingleUse());
    assertEquals(QueryMode.PLAN, selectRequests.get(0).getQueryMode());
    assertTrue(selectRequests.get(1).getTransaction().hasSingleUse());
    assertEquals(QueryMode.NORMAL, selectRequests.get(1).getQueryMode());

    List<ExecuteSqlRequest> updateRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().startsWith("UPDATE"))
            .collect(Collectors.toList());
    assertEquals(2, updateRequests.size());
    assertEquals(getUpdateSingerStatement(), updateRequests.get(0).getSql());
    assertEquals(QueryMode.PLAN, updateRequests.get(0).getQueryMode());
    assertTrue(updateRequests.get(0).getTransaction().hasBegin());
    assertEquals(getUpdateSingerStatement(), updateRequests.get(1).getSql());
    assertEquals(QueryMode.NORMAL, updateRequests.get(1).getQueryMode());
    assertTrue(updateRequests.get(1).getTransaction().hasId());
  }

  static void addInsertSingerResult(Singer singer, boolean exact) {
    String sql =
        "INSERT INTO \"singers\" (\"singer_id\", \"first_name\", \"last_name\", \"active\", \"created_at\", \"updated_at\", \"lock_version\") "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING \"singer_id\"";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("singer_id")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p2")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p3")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p4")
                            .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p5")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p6")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p7")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));

    StatementResult result =
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(singer.singerId)
                .bind("p2")
                .to(singer.firstName)
                .bind("p3")
                .to(singer.lastName)
                .bind("p4")
                .to(singer.active)
                .bind("p5")
                .to(singer.createdAt)
                .bind("p6")
                .to(singer.updatedAt)
                .bind("p7")
                .to(singer.version)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(singer.singerId).build())
                        .build())
                .build());
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
  }

  static void addSelectSingersResult(ImmutableList<Singer> singers) {
    String sql = "SELECT \"singers\".* FROM \"singers\"";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createSelectSingersMetadata())
                .addAllRows(
                    singers.stream()
                        .map(ActiveRecordMockServerTest::createSingerRecord)
                        .collect(Collectors.toList()))
                .build()));
  }

  static String getSelectSingerSql() {
    return "SELECT \"singers\".* FROM \"singers\" WHERE \"singers\".\"singer_id\" = $1 LIMIT $2";
  }

  static void addSelectSingerResult(Singer singer) {
    String sql = getSelectSingerSql();
    ResultSetMetadata metadata = createSelectSingerMetadata();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ResultSet.newBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(singer.singerId).bind("p2").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(createSingerRecord(singer))
                .build()));
  }

  static ResultSetMetadata createSelectSingersMetadata() {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName("singer_id")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("first_name")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("last_name")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("full_name")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("active")
                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("created_at")
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("updated_at")
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("lock_version")
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .build())
                .build())
        .build();
  }

  static ResultSetMetadata createSelectSingerMetadata() {
    return addSelectOneParameters(createSelectSingersMetadata());
  }

  static ListValue createSingerRecord(Singer singer) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(singer.singerId).build())
        .addValues(Value.newBuilder().setStringValue(singer.firstName).build())
        .addValues(Value.newBuilder().setStringValue(singer.lastName).build())
        .addValues(
            Value.newBuilder().setStringValue(singer.firstName + " " + singer.lastName).build())
        .addValues(Value.newBuilder().setBoolValue(singer.active).build())
        .addValues(Value.newBuilder().setStringValue(singer.createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(singer.updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(singer.version)).build())
        .build();
  }

  static String getUpdateSingerStatement() {
    return "UPDATE \"singers\" SET \"first_name\" = $1, \"last_name\" = $2, \"updated_at\" = $3, \"lock_version\" = $4 "
        + "WHERE \"singers\".\"singer_id\" = $5 AND \"singers\".\"lock_version\" = $6";
  }

  static void addUpdateSingerResult(Singer singer, boolean exact) {
    String sql = getUpdateSingerStatement();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.TIMESTAMP,
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    StatementResult result =
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(singer.firstName)
                .bind("p2")
                .to(singer.lastName)
                .bind("p3")
                .to(singer.updatedAt)
                .bind("p4")
                .to(singer.version + 1)
                .bind("p5")
                .to(singer.singerId)
                .bind("p6")
                .to(singer.version)
                .build(),
            1L);
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
  }

  static void addInsertAlbumResult(Album album, boolean exact) {
    String sql =
        "INSERT INTO \"albums\" (\"album_id\", \"title\", \"singer_id\") "
            + "VALUES ($1, $2, $3) RETURNING \"album_id\"";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("album_id")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p2")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p3")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    StatementResult result =
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(album.albumId)
                .bind("p2")
                .to(album.title)
                .bind("p3")
                .to(album.singerId)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(album.albumId).build())
                        .build())
                .build());
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
  }

  static void addSelectAlbumsOfSingerResult(
      String singerId, ImmutableList<Album> albums, boolean exact) {
    String sql = "SELECT \"albums\".* FROM \"albums\" WHERE \"albums\".\"singer_id\" = $1";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("album_id")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("title")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("singer_id")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ResultSet.newBuilder().setMetadata(metadata).build()));

    StatementResult result =
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(singerId).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addAllRows(
                    albums.stream()
                        .map(ActiveRecordMockServerTest::createAlbumRecord)
                        .collect(Collectors.toList()))
                .build());
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
  }

  static ListValue createAlbumRecord(Album album) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(album.albumId).build())
        .addValues(Value.newBuilder().setStringValue(album.title).build())
        .addValues(Value.newBuilder().setStringValue(album.singerId).build())
        .build();
  }

  static ResultSetMetadata addSelectOneParameters(ResultSetMetadata metadata) {
    return metadata
        .toBuilder()
        .setUndeclaredParameters(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName("p1")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setName("p2")
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .build())
                .build())
        .build();
  }

  static void addSelectSampleTablesResult() {
    addSelectTablesResult("ar_internal_metadata", "schema_migrations", "albums", "singers");

    addSelectColumnsResult(
        "singers",
        createColumnRow("singer_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("first_name", "character varying", false, Oid.VARCHAR),
        createColumnRow("last_name", "character varying", true, Oid.VARCHAR),
        createColumnRow("full_name", "character varying", false, Oid.VARCHAR),
        createColumnRow("active", "boolean", false, Oid.BOOL),
        createColumnRow("created_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("updated_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("lock_version", "bigint", false, Oid.INT8));
    addSelectPrimaryKeyResult("singers", "singer_id");
    addSelectTableDescriptionResult("singers", "create table singers");
    // TODO: Actually add indexes.
    addSelectTableIndexesResult("singers");
    addSelectTableConstraintsResult("singers");

    addSelectColumnsResult(
        "albums",
        createColumnRow("album_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("title", "character varying", true, Oid.VARCHAR),
        createColumnRow("singer_id", "character varying", true, Oid.VARCHAR));
    addSelectPrimaryKeyResult("albums", "album_id");
    addSelectTableDescriptionResult("albums", "create table albums");
    // TODO: Actually add indexes.
    addSelectTableIndexesResult("albums");
    addSelectTableConstraintsResult("albums");

    // TODO: Actually add foreign keys.
    addSelectTableForeignKeysResult("albums");
    addSelectTableForeignKeysResult("singers");
  }
}
