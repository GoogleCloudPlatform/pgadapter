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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.math.BigDecimal;
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
  private abstract static class Model {
    Timestamp createdAt;
    Timestamp updatedAt;
    long version;

    Model(Timestamp createdAt, Timestamp updatedAt, long version) {
      this.createdAt = createdAt;
      this.updatedAt = updatedAt;
      this.version = version;
    }
  }

  private static final class Singer extends Model {
    final String singerId;
    String firstName;
    String lastName;
    boolean active;

    Singer(
        String singerId,
        String firstName,
        String lastName,
        boolean active,
        Timestamp createdAt,
        Timestamp updatedAt,
        long version) {
      super(createdAt, updatedAt, version);
      this.singerId = singerId;
      this.firstName = firstName;
      this.lastName = lastName;
      this.active = active;
    }
  }

  private static final class Album extends Model {
    final String albumId;
    String title;
    BigDecimal marketingBudget;
    Date releaseDate;
    ByteArray coverPicture;
    String singerId;

    Album(
        String albumId,
        String title,
        BigDecimal marketingBudget,
        Date releaseDate,
        ByteArray coverPicture,
        String singerId,
        Timestamp createdAt,
        Timestamp updatedAt,
        long version) {
      super(createdAt, updatedAt, version);
      this.albumId = albumId;
      this.title = title;
      this.marketingBudget = marketingBudget;
      this.releaseDate = releaseDate;
      this.coverPicture = coverPicture;
      this.singerId = singerId;
    }
  }

  private static final class Track extends Model {
    final String albumId;
    final long trackNumber;
    String title;
    Double sampleRate;

    Track(
        String albumId,
        long trackNumber,
        String title,
        Double sampleRate,
        Timestamp createdAt,
        Timestamp updatedAt,
        long version) {
      super(createdAt, updatedAt, version);
      this.albumId = albumId;
      this.trackNumber = trackNumber;
      this.title = title;
      this.sampleRate = sampleRate;
    }
  }

  private static final class Venue extends Model {
    final String venueId;
    String name;
    String description;

    Venue(
        String venueId,
        String name,
        String description,
        Timestamp createdAt,
        Timestamp updatedAt,
        long version) {
      super(createdAt, updatedAt, version);
      this.venueId = venueId;
      this.name = name;
      this.description = description;
    }
  }

  private static final class Concert extends Model {
    final String concertId;
    String venueId;
    String singerId;
    String name;
    Timestamp startTime;
    Timestamp endTime;

    Concert(
        String concertId,
        String venueId,
        String singerId,
        String name,
        Timestamp startTime,
        Timestamp endTime,
        Timestamp createdAt,
        Timestamp updatedAt,
        long version) {
      super(createdAt, updatedAt, version);
      this.concertId = concertId;
      this.venueId = venueId;
      this.singerId = singerId;
      this.name = name;
      this.startTime = startTime;
      this.endTime = endTime;
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
    assertEquals(5, request.getStatementsCount());
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
    assertEquals(
        "create table tracks (\n"
            + "        album_id     varchar(36) not null,\n"
            + "        track_number bigint not null,\n"
            + "        title        varchar not null,\n"
            + "        sample_rate  float8 not null,\n"
            + "        created_at   timestamptz,\n"
            + "        updated_at   timestamptz,\n"
            + "        lock_version bigint not null,\n"
            + "        primary key (album_id, track_number)\n"
            + "    ) interleave in parent albums on delete cascade",
        request.getStatements(2));
    assertEquals(
        "CREATE TABLE \"venues\" ("
            + "\"venue_id\" character varying(36) NOT NULL PRIMARY KEY, "
            + "\"name\" character varying, "
            + "\"description\" jsonb, "
            + "\"created_at\" timestamptz, "
            + "\"updated_at\" timestamptz, "
            + "\"lock_version\" integer NOT NULL)",
        request.getStatements(3));
    assertEquals(
        "CREATE TABLE \"concerts\" ("
            + "\"concert_id\" character varying(36) NOT NULL PRIMARY KEY, "
            + "\"venue_id\" character varying(36), "
            + "\"singer_id\" character varying(36), "
            + "\"name\" character varying, "
            + "\"start_time\" timestamptz NOT NULL, "
            + "\"end_time\" timestamptz NOT NULL, "
            + "\"created_at\" timestamptz, "
            + "\"updated_at\" timestamptz, "
            + "\"lock_version\" integer NOT NULL, "
            + "CONSTRAINT \"fk_rails_a72d62761d\"\n"
            + "FOREIGN KEY (\"venue_id\")\n"
            + "  REFERENCES \"venues\" (\"venue_id\")\n"
            + ", CONSTRAINT \"fk_rails_20a8d5418f\"\n"
            + "FOREIGN KEY (\"singer_id\")\n"
            + "  REFERENCES \"singers\" (\"singer_id\")\n"
            + ", CONSTRAINT chk_end_time_after_start_time CHECK (end_time > start_time))",
        request.getStatements(4));
  }

  @Test
  public void testRakeRun() throws Exception {
    addMigrationsTableResults();
    addSelectSampleTablesResult();
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.of("DELETE FROM \"albums\""), 0L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.of("DELETE FROM \"singers\""), 0L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.of("DELETE FROM \"concerts\""), 0L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.of("DELETE FROM \"venues\""), 0L));
    addInspectSingerResult(ImmutableList.of());

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

    Album album =
        new Album(
            randomUuid(),
            "some-title",
            new BigDecimal("200000.12"),
            Date.fromYearMonthDay(2000, 1, 1),
            ByteArray.copyFrom("test"),
            singer.singerId,
            Timestamp.now(),
            Timestamp.now(),
            1);
    addInsertAlbumResult(album, false);
    addSelectAlbumsOfSingerResult(
        singer.singerId,
        ImmutableList.of(
            new Album(
                randomUuid(),
                "some-title-1",
                new BigDecimal("200000.12"),
                Date.fromYearMonthDay(2000, 1, 1),
                ByteArray.copyFrom("test"),
                singer.singerId,
                Timestamp.now(),
                Timestamp.now(),
                1),
            new Album(
                randomUuid(),
                "some-title-2",
                new BigDecimal("200000.12"),
                Date.fromYearMonthDay(2000, 1, 1),
                ByteArray.copyFrom("test"),
                singer.singerId,
                Timestamp.now(),
                Timestamp.now(),
                1),
            new Album(
                randomUuid(),
                "some-title-3",
                new BigDecimal("200000.12"),
                Date.fromYearMonthDay(2000, 1, 1),
                ByteArray.copyFrom("test"),
                singer.singerId,
                Timestamp.now(),
                Timestamp.now(),
                1)),
        true);
    Track track =
        new Track(album.albumId, 1L, "some-title", 3.14d, Timestamp.now(), Timestamp.now(), 1);
    addInsertTrackResult(track, false);
    addSelectTracksOfAlbumResult(
        album.albumId,
        ImmutableList.of(
            new Track(
                track.albumId,
                1L,
                "some-track-title-1",
                3.14d,
                Timestamp.now(),
                Timestamp.now(),
                1),
            new Track(
                track.albumId,
                2L,
                "some-track-title-2",
                99.9d,
                Timestamp.now(),
                Timestamp.now(),
                1),
            new Track(
                track.albumId,
                3L,
                "some-track-title-3",
                -1.2d,
                Timestamp.now(),
                Timestamp.now(),
                1)),
        false);

    Venue venue =
        new Venue(
            randomUuid(),
            "some-venue",
            "{\"Capacity\": 1000}",
            Timestamp.now(),
            Timestamp.now(),
            1);
    addInsertVenueResult(venue, false);
    Concert concert =
        new Concert(
            randomUuid(),
            venue.venueId,
            singer.singerId,
            "some-concert",
            Timestamp.parseTimestamp("2022-04-01T10:00:00Z"),
            Timestamp.parseTimestamp("2022-04-01T15:00:00Z"),
            Timestamp.now(),
            Timestamp.now(),
            1);
    addInsertConcertResult(concert, false);

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

    String output =
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

    assertTrue(output, output.contains("some-track-title-1"));

    List<ExecuteSqlRequest> selectSingerRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(getSelectSingerSql()))
            .collect(Collectors.toList());
    assertEquals(2, selectSingerRequests.size());
    assertTrue(selectSingerRequests.get(0).getTransaction().hasSingleUse());
    assertEquals(QueryMode.PLAN, selectSingerRequests.get(0).getQueryMode());
    assertTrue(selectSingerRequests.get(1).getTransaction().hasSingleUse());
    assertEquals(QueryMode.NORMAL, selectSingerRequests.get(1).getQueryMode());

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

    List<ExecuteSqlRequest> insertSingersRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(getInsertSingerStatement()))
            .collect(Collectors.toList());
    assertEquals(11, insertSingersRequests.size());
    assertEquals(QueryMode.PLAN, insertSingersRequests.get(0).getQueryMode());
    assertTrue(insertSingersRequests.get(0).getTransaction().hasId());
    for (int i = 1; i < insertSingersRequests.size(); i++) {
      assertEquals(QueryMode.NORMAL, insertSingersRequests.get(i).getQueryMode());
      assertTrue(insertSingersRequests.get(i).getTransaction().hasId());
      assertEquals(
          insertSingersRequests.get(i - 1).getTransaction().getId(),
          insertSingersRequests.get(i).getTransaction().getId());
    }

    List<ExecuteSqlRequest> insertAlbumRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(getInsertAlbumStatement()))
            .collect(Collectors.toList());
    assertEquals(31, insertAlbumRequests.size());
    assertEquals(QueryMode.PLAN, insertAlbumRequests.get(0).getQueryMode());
    assertTrue(insertAlbumRequests.get(0).getTransaction().hasId());
    for (int i = 1; i < insertAlbumRequests.size(); i++) {
      assertEquals(QueryMode.NORMAL, insertAlbumRequests.get(i).getQueryMode());
      assertTrue(insertAlbumRequests.get(i).getTransaction().hasId());
      assertEquals(
          insertAlbumRequests.get(i - 1).getTransaction().getId(),
          insertAlbumRequests.get(i).getTransaction().getId());
    }
    assertEquals(
        insertAlbumRequests.get(0).getTransaction().getId(),
        insertSingersRequests.get(0).getTransaction().getId());
    assertEquals(
        1,
        mockSpanner.getRequestsOfType(CommitRequest.class).stream()
            .filter(
                request ->
                    request
                        .getTransactionId()
                        .equals(insertSingersRequests.get(0).getTransaction().getId()))
            .count());
  }

  static String getInsertSingerStatement() {
    return "INSERT INTO \"singers\" (\"singer_id\", \"first_name\", \"last_name\", \"active\", \"created_at\", \"updated_at\", \"lock_version\") "
        + "VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING \"singer_id\"";
  }

  static void addInsertSingerResult(Singer singer, boolean exact) {
    String sql = getInsertSingerStatement();
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

  static void addInspectSingerResult(ImmutableList<Singer> singers) {
    String sql = "SELECT \"singers\".* FROM \"singers\" /* loading for inspect */ LIMIT $1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder().setMetadata(createSelectSingersMetadata()).build()));
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

  static String getInsertAlbumStatement() {
    return "INSERT INTO \"albums\" (\"album_id\", \"title\", \"marketing_budget\", \"release_date\", \"cover_picture\", \"singer_id\", \"created_at\", \"updated_at\", \"lock_version\") "
        + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING \"album_id\"";
  }

  static void addInsertAlbumResult(Album album, boolean exact) {
    String sql = getInsertAlbumStatement();
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
                            .setType(Type.newBuilder().setCode(TypeCode.NUMERIC).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p4")
                            .setType(Type.newBuilder().setCode(TypeCode.DATE).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p5")
                            .setType(Type.newBuilder().setCode(TypeCode.BYTES).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p6")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p7")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p8")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p9")
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
                .to(album.albumId)
                .bind("p2")
                .to(album.title)
                .bind("p3")
                .to(album.marketingBudget)
                .bind("p4")
                .to(album.releaseDate)
                .bind("p5")
                .to(album.coverPicture)
                .bind("p6")
                .to(album.singerId)
                .bind("p7")
                .to(album.createdAt)
                .bind("p8")
                .to(album.updatedAt)
                .bind("p9")
                .to(album.version)
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

  static void addSelectTracksOfAlbumResult(
      String albumId, ImmutableList<Track> tracks, boolean exact) {
    String sql = "SELECT \"tracks\".* FROM \"tracks\" WHERE \"tracks\".\"album_id\" = $1";
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
                            .setName("track_number")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("title")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("sample_rate")
                            .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build())
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
            Statement.newBuilder(sql).bind("p1").to(albumId).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addAllRows(
                    tracks.stream()
                        .map(ActiveRecordMockServerTest::createTrackRecord)
                        .collect(Collectors.toList()))
                .build());
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
  }

  static ListValue createTrackRecord(Track track) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(track.albumId).build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(track.trackNumber)).build())
        .addValues(Value.newBuilder().setStringValue(track.title).build())
        .addValues(Value.newBuilder().setNumberValue(track.sampleRate).build())
        .addValues(Value.newBuilder().setStringValue(track.createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(track.updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(track.version)).build())
        .build();
  }

  static String getInsertTrackStatement() {
    return "INSERT INTO \"tracks\" (\"album_id\", \"track_number\", \"title\", \"sample_rate\", \"created_at\", \"updated_at\", \"lock_version\") "
        + "VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING \"album_id\",\"track_number\"";
  }

  static void addInsertTrackResult(Track album, boolean exact) {
    String sql = getInsertTrackStatement();
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
                            .setName("track_number")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
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
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p3")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p4")
                            .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build())
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
                .to(album.albumId)
                .bind("p2")
                .to(album.trackNumber)
                .bind("p3")
                .to(album.title)
                .bind("p4")
                .to(album.sampleRate)
                .bind("p5")
                .to(album.createdAt)
                .bind("p6")
                .to(album.updatedAt)
                .bind("p7")
                .to(album.version)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(album.albumId).build())
                        .addValues(
                            Value.newBuilder()
                                .setStringValue(String.valueOf(album.trackNumber))
                                .build())
                        .build())
                .build());
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
  }

  static String getInsertVenueStatement() {
    return "INSERT INTO \"venues\" (\"venue_id\", \"name\", \"description\", \"created_at\", \"updated_at\", \"lock_version\") "
        + "VALUES ($1, $2, $3, $4, $5, $6) RETURNING \"venue_id\"";
  }

  static void addInsertVenueResult(Venue venue, boolean exact) {
    String sql = getInsertVenueStatement();
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("venue_id")
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
                            .setType(Type.newBuilder().setCode(TypeCode.JSON).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p4")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p5")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p6")
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
                .to(venue.venueId)
                .bind("p2")
                .to(venue.name)
                .bind("p3")
                .to(venue.description)
                .bind("p4")
                .to(venue.createdAt)
                .bind("p5")
                .to(venue.updatedAt)
                .bind("p6")
                .to(venue.version)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(venue.venueId).build())
                        .build())
                .build());
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
  }

  static String getInsertConcertStatement() {
    return "INSERT INTO \"concerts\" (\"concert_id\", \"venue_id\", \"singer_id\", \"name\", \"start_time\", \"end_time\", \"created_at\", \"updated_at\", \"lock_version\") "
        + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING \"concert_id\"";
  }

  static void addInsertConcertResult(Concert concert, boolean exact) {
    String sql = getInsertConcertStatement();
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("concert_id")
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
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
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
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p8")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("p9")
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
                .to(concert.concertId)
                .bind("p2")
                .to(concert.venueId)
                .bind("p3")
                .to(concert.singerId)
                .bind("p4")
                .to(concert.name)
                .bind("p5")
                .to(concert.startTime)
                .bind("p6")
                .to(concert.endTime)
                .bind("p7")
                .to(concert.createdAt)
                .bind("p8")
                .to(concert.updatedAt)
                .bind("p9")
                .to(concert.version)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(concert.concertId).build())
                        .build())
                .build());
    if (exact) {
      mockSpanner.putStatementResult(result);
    } else {
      mockSpanner.putPartialStatementResult(result);
    }
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
    addSelectTablesResult(
        "ar_internal_metadata",
        "schema_migrations",
        "albums",
        "concerts",
        "singers",
        "tracks",
        "venues");

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
        createColumnRow("marketing_budget", "numeric", false, Oid.NUMERIC),
        createColumnRow("release_date", "date", false, Oid.DATE),
        createColumnRow("cover_picture", "bytea", false, Oid.BYTEA),
        createColumnRow("singer_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("created_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("updated_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("lock_version", "bigint", false, Oid.INT8));
    addSelectPrimaryKeyResult("albums", "album_id");
    addSelectTableDescriptionResult("albums", "create table albums");
    // TODO: Actually add indexes.
    addSelectTableIndexesResult("albums");
    addSelectTableConstraintsResult("albums");

    addSelectColumnsResult(
        "tracks",
        createColumnRow("album_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("track_number", "bigint", true, Oid.INT8),
        createColumnRow("title", "character varying", true, Oid.VARCHAR),
        createColumnRow("sample_rate", "double precision", false, Oid.FLOAT8),
        createColumnRow("created_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("updated_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("lock_version", "bigint", false, Oid.INT8));
    addSelectPrimaryKeyResult("tracks", "album_id", "track_number");
    addSelectTableDescriptionResult("tracks", "create table tracks");
    // TODO: Actually add indexes.
    addSelectTableIndexesResult("tracks");
    addSelectTableConstraintsResult("tracks");

    addSelectColumnsResult(
        "venues",
        createColumnRow("venue_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("name", "character varying", false, Oid.VARCHAR),
        createColumnRow("description", "jsonb", false, Oid.VARCHAR),
        createColumnRow("created_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("updated_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("lock_version", "bigint", false, Oid.INT8));
    addSelectPrimaryKeyResult("venues", "venue_id");
    addSelectTableDescriptionResult("venues", "create table venues");
    // TODO: Actually add indexes.
    addSelectTableIndexesResult("venues");
    addSelectTableConstraintsResult("venues");

    addSelectColumnsResult(
        "concerts",
        createColumnRow("concert_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("venue_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("singer_id", "character varying", true, Oid.VARCHAR),
        createColumnRow("name", "character varying", true, Oid.VARCHAR),
        createColumnRow("start_time", "timestamp with time zone", true, Oid.TIMESTAMPTZ),
        createColumnRow("end_time", "timestamp with time zone", true, Oid.TIMESTAMPTZ),
        createColumnRow("created_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("updated_at", "timestamp with time zone", false, Oid.TIMESTAMPTZ),
        createColumnRow("lock_version", "bigint", false, Oid.INT8));
    addSelectPrimaryKeyResult("concerts", "concert_id");
    addSelectTableDescriptionResult("concerts", "create table concerts");
    // TODO: Actually add indexes.
    addSelectTableIndexesResult("concerts");
    addSelectTableConstraintsResult("concerts");

    // TODO: Actually add foreign keys.
    addSelectTableForeignKeysResult("albums", true);
    addSelectTableForeignKeysResult("singers", true);
    addSelectTableForeignKeysResult("tracks", true);
    addSelectTableForeignKeysResult("venues", true);
    addSelectTableForeignKeysResult("concerts", true);
  }
}
