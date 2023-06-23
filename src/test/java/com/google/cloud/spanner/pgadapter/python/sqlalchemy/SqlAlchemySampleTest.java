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

package com.google.cloud.spanner.pgadapter.python.sqlalchemy;

import static com.google.cloud.spanner.pgadapter.python.sqlalchemy.SqlAlchemyBasicsTest.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.protobuf.Duration;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PythonTest.class)
public class SqlAlchemySampleTest extends AbstractMockServerTest {
  private static final String SAMPLE_DIR = "./samples/python/sqlalchemy-sample";

  @BeforeClass
  public static void setupBaseResults() throws Exception {
    SqlAlchemyBasicsTest.setupBaseResults(SAMPLE_DIR);
  }

  @Test
  public void testDeleteAlbum() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE albums.id = '123-456-789'"),
            ResultSet.newBuilder()
                .setMetadata(createAlbumsMetadata("albums_"))
                .addRows(
                    createAlbumRow(
                        "123-456-789",
                        "My album",
                        "5000",
                        Date.parseDate("2000-01-01"),
                        ByteArray.copyFrom("My album cover picture"),
                        "321",
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z")))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.of(
                "DELETE FROM albums WHERE albums.id = '123-456-789' AND albums.version_id = 1"),
            1L));

    String output =
        execute(SAMPLE_DIR, "test_delete_album.py", "localhost", pgServer.getLocalPort());
    assertEquals("\n" + "Deleted album with id 123-456-789\n", output);
  }

  @Test
  public void testAlbumsWithTitleFirstCharEqualToSingerName() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums JOIN singers ON singers.id = albums.singer_id \n"
                    + "WHERE lower(SUBSTRING(albums.title FROM 1 FOR 1)) = lower(SUBSTRING(singers.first_name FROM 1 FOR 1)) OR "
                    + "lower(SUBSTRING(albums.title FROM 1 FOR 1)) = lower(SUBSTRING(singers.last_name FROM 1 FOR 1))"),
            ResultSet.newBuilder().setMetadata(createAlbumsMetadata("albums_")).build()));

    String output =
        execute(
            SAMPLE_DIR,
            "test_print_albums_first_character_of_title_equal_to_first_or_last_name.py",
            "localhost",
            pgServer.getLocalPort());
  }

  @Test
  public void testSingersWithLimitAndOffset() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at, singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active \n"
                    + "FROM singers ORDER BY singers.last_name \n"
                    + " LIMIT 5 OFFSET 3"),
            ResultSet.newBuilder()
                .setMetadata(createSingersMetadata("singers_"))
                .addRows(
                    createSingerRow(
                        "123",
                        "Pete",
                        "Allison",
                        true,
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z")))
                .addRows(
                    createSingerRow(
                        "321",
                        "Alice",
                        "Henderson",
                        true,
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z")))
                .build()));
    String output =
        execute(
            SAMPLE_DIR,
            "test_print_singers_with_limit_and_offset.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals(
        "\n"
            + "Printing at most 5 singers ordered by last name\n"
            + "  1. Pete Allison\n"
            + "  2. Alice Henderson\n"
            + "Found 2 singers\n",
        output);
  }

  @Test
  public void testPrintAlbumsReleasedBefore1980() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE albums.release_date < '1980-01-01'::date"),
            ResultSet.newBuilder()
                .setMetadata(createAlbumsMetadata("albums_"))
                .addRows(
                    createAlbumRow(
                        "a1",
                        "Album 1",
                        "123.456",
                        Date.parseDate("1979-10-16"),
                        ByteArray.copyFrom("some cover picture"),
                        "123",
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z")))
                .build()));
    String output =
        execute(
            SAMPLE_DIR,
            "test_print_albums_released_before_1980.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals(
        "\n"
            + "Searching for albums released before 1980\n"
            + "  Album Album 1 was released at 1979-10-16\n",
        output);
  }

  @Test
  public void testPrintConcerts() throws Exception {
    List<Field> concertsFields = createConcertsMetadata("concerts_").getRowType().getFieldsList();
    List<Field> venuesFields = createVenuesMetadata("venues_1_").getRowType().getFieldsList();
    List<Field> singersFields = createSingersMetadata("singers_1_").getRowType().getFieldsList();
    List<Value> concertValues =
        createConcertRow(
                "c1",
                "Avenue Park Open",
                "v1",
                "123",
                Timestamp.parseTimestamp("2023-02-01T20:00:00-05:00"),
                Timestamp.parseTimestamp("2023-02-02T02:00:00-05:00"),
                Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                Timestamp.parseTimestamp("2022-12-02T17:30:00Z"))
            .getValuesList();
    List<Value> venueValues =
        createVenueRow(
                "v1",
                "Avenue Park",
                "{\n"
                    + "  \"Capacity\": 5000,\n"
                    + "  \"Location\": \"New York\",\n"
                    + "  \"Country\": \"US\"\n"
                    + "}",
                Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                Timestamp.parseTimestamp("2022-12-02T17:30:00Z"))
            .getValuesList();
    List<Value> singerValues =
        createSingerRow(
                "123",
                "Pete",
                "Allison",
                true,
                Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                Timestamp.parseTimestamp("2022-12-02T17:30:00Z"))
            .getValuesList();

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT concerts.id AS concerts_id, concerts.version_id AS concerts_version_id, concerts.created_at AS concerts_created_at, concerts.updated_at AS concerts_updated_at, concerts.name AS concerts_name, concerts.venue_id AS concerts_venue_id, concerts.singer_id AS concerts_singer_id, concerts.start_time AS concerts_start_time, concerts.end_time AS concerts_end_time, "
                    + "venues_1.id AS venues_1_id, venues_1.version_id AS venues_1_version_id, venues_1.created_at AS venues_1_created_at, venues_1.updated_at AS venues_1_updated_at, venues_1.name AS venues_1_name, venues_1.description AS venues_1_description, "
                    + "singers_1.id AS singers_1_id, singers_1.version_id AS singers_1_version_id, singers_1.created_at AS singers_1_created_at, singers_1.updated_at AS singers_1_updated_at, singers_1.first_name AS singers_1_first_name, singers_1.last_name AS singers_1_last_name, singers_1.full_name AS singers_1_full_name, singers_1.active AS singers_1_active \n"
                    + "FROM concerts LEFT OUTER JOIN venues AS venues_1 ON venues_1.id = concerts.venue_id LEFT OUTER JOIN singers AS singers_1 ON singers_1.id = concerts.singer_id ORDER BY concerts.start_time"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addAllFields(concertsFields)
                                .addAllFields(venuesFields)
                                .addAllFields(singersFields))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addAllValues(concertValues)
                        .addAllValues(venueValues)
                        .addAllValues(singerValues)
                        .build())
                .build()));
    String output =
        execute(SAMPLE_DIR, "test_print_concerts.py", "localhost", pgServer.getLocalPort());
    assertEquals(
        "\nConcert 'Avenue Park Open' starting at 2023-02-02 02:00:00+01:00 with Pete Allison will be held at Avenue Park\n",
        output);
  }

  @Test
  public void testCreateVenueAndConcertInTransaction() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at, singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active \n"
                    + "FROM singers \n"
                    + " LIMIT 1"),
            ResultSet.newBuilder()
                .setMetadata(createSingersMetadata("singers_"))
                .addRows(
                    createSingerRow(
                        "123",
                        "Pete",
                        "Allison",
                        true,
                        Timestamp.parseTimestamp("2001-02-28T00:00:00Z"),
                        Timestamp.parseTimestamp("2001-02-28T00:00:00Z")))
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.update(
            Statement.of(
                "INSERT INTO venues (id, version_id, created_at, updated_at, name, description) VALUES "),
            1L));
    mockSpanner.putPartialStatementResult(
        StatementResult.update(
            Statement.of(
                "INSERT INTO concerts (id, version_id, created_at, updated_at, name, venue_id, singer_id, start_time, end_time) VALUES "),
            1L));

    String output =
        execute(
            SAMPLE_DIR,
            "test_create_venue_and_concert_in_transaction.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals("\nCreated Venue and Concert\n", output);
  }

  @Test
  public void testCreateRandomSingersAndAlbums() throws Exception {
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO singers (id, version_id, created_at, updated_at, first_name, last_name, active) "
                    + "VALUES "),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("full_name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build()))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(5L).build())
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.update(
            Statement.of(
                "INSERT INTO albums (id, version_id, created_at, updated_at, title, marketing_budget, release_date, cover_picture, singer_id) VALUES "),
            37L));

    String output =
        execute(
            SAMPLE_DIR,
            "test_create_random_singers_and_albums.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals("Created 5 singers\n", output);
  }

  @Test
  public void testPrintSingersAndAlbums() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at, singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active \n"
                    + "FROM singers ORDER BY singers.last_name"),
            ResultSet.newBuilder()
                .setMetadata(createSingersMetadata("singers_"))
                .addRows(
                    createSingerRow(
                        "b2",
                        "Pete",
                        "Allison",
                        true,
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z"),
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z")))
                .addRows(
                    createSingerRow(
                        "a1",
                        "Alice",
                        "Henderson",
                        true,
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z"),
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z")))
                .addRows(
                    createSingerRow(
                        "c3",
                        "Renate",
                        "Unna",
                        true,
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z"),
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z")))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE 'b2' = albums.singer_id"),
            ResultSet.newBuilder()
                .setMetadata(createAlbumsMetadata("albums_"))
                .addRows(
                    createAlbumRow(
                        "a1",
                        "Title 1",
                        "100.90",
                        Date.parseDate("2000-01-01"),
                        ByteArray.copyFrom("cover pic 1"),
                        "b2",
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z"),
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z")))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE 'a1' = albums.singer_id"),
            ResultSet.newBuilder()
                .setMetadata(createAlbumsMetadata("albums_"))
                .addRows(
                    createAlbumRow(
                        "a2",
                        "Title 2",
                        "100.90",
                        Date.parseDate("2000-01-01"),
                        ByteArray.copyFrom("cover pic 1"),
                        "a1",
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z"),
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z")))
                .addRows(
                    createAlbumRow(
                        "a3",
                        "Title 3",
                        "100.90",
                        Date.parseDate("2000-01-01"),
                        ByteArray.copyFrom("cover pic 2"),
                        "a1",
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z"),
                        Timestamp.parseTimestamp("2022-12-01T15:12:00Z")))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE 'c3' = albums.singer_id"),
            ResultSet.newBuilder().setMetadata(createAlbumsMetadata("albums_")).build()));

    String output =
        execute(
            SAMPLE_DIR, "test_print_singers_and_albums.py", "localhost", pgServer.getLocalPort());
    assertEquals(
        "\n"
            + "Pete Allison has 1 albums:\n"
            + "  'Title 1'\n"
            + "Alice Henderson has 2 albums:\n"
            + "  'Title 2'\n"
            + "  'Title 3'\n"
            + "Renate Unna has 0 albums:\n",
        output);
    List<BeginTransactionRequest> beginRequests =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class);
    assertEquals(1, beginRequests.size());
    assertTrue(beginRequests.get(0).getOptions().hasReadOnly());
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(5, requests.size());
  }

  @Test
  public void testGetSinger() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at, singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active \n"
                    + "FROM singers \n"
                    + "WHERE singers.id = '123-456-789'"),
            ResultSet.newBuilder()
                .setMetadata(createSingersMetadata("singers_"))
                .addRows(
                    createSingerRow(
                        "123-456-789",
                        "Myfirstname",
                        "Mylastname",
                        true,
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z"),
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z")))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE '123-456-789' = albums.singer_id"),
            ResultSet.newBuilder()
                .setMetadata(createAlbumsMetadata("albums_"))
                .addRows(
                    createAlbumRow(
                        "987-654-321",
                        "My title",
                        "9423.13",
                        Date.parseDate("2002-10-17"),
                        ByteArray.copyFrom("cover picture"),
                        "123-456-789",
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z"),
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z")))
                .build()));

    String output = execute(SAMPLE_DIR, "test_get_singer.py", "localhost", pgServer.getLocalPort());
    assertEquals(
        "singers(id='123-456-789',first_name='Myfirstname',last_name='Mylastname',active=True,created_at='2022-02-21T10:19:18+00:00',updated_at='2022-02-21T10:19:18+00:00')\n"
            + "Albums:\n"
            + "[albums(id='987-654-321',title='My title',marketing_budget=Decimal('9423.13'),release_date=datetime.date(2002, 10, 17),cover_picture=b'cover picture',singer='123-456-789',created_at='2022-02-21T10:19:18+00:00',updated_at='2022-02-21T10:19:18+00:00')]\n",
        output);
  }

  @Test
  public void testAddSinger() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO singers (id, version_id, created_at, updated_at, first_name, last_name, active) "
                    + "VALUES ('123-456-789', 1, ('2011-11-04T00:05:23.123456+00:00'::timestamptz), NULL, 'Myfirstname', 'Mylastname', true) "
                    + "RETURNING singers.full_name"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("full_name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build()))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder().setStringValue("Myfirstname Mylastname").build())
                        .build())
                .build()));

    String output = execute(SAMPLE_DIR, "test_add_singer.py", "localhost", pgServer.getLocalPort());
    assertEquals("Added singer 123-456-789 with full name Myfirstname Mylastname\n", output);
  }

  @Test
  public void testUpdateSinger() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at, singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active \n"
                    + "FROM singers \n"
                    + "WHERE singers.id = '123-456-789'"),
            ResultSet.newBuilder()
                .setMetadata(createSingersMetadata("singers_"))
                .addRows(
                    createSingerRow(
                        "123-456-789",
                        "Myfirstname",
                        "Mylastname",
                        true,
                        Timestamp.parseTimestamp("2022-12-01T10:00:00Z"),
                        Timestamp.parseTimestamp("2022-12-01T10:00:00Z")))
                .build()));
    // We have to use a partial SQL string here, as we don't know exactly what updated_at timestamp
    // will be used by SQLAlchemy.
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of("UPDATE singers SET version_id=2, updated_at='"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("full_name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build()))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder().setStringValue("Newfirstname Newlastname").build())
                        .build())
                .build()));

    String output =
        execute(SAMPLE_DIR, "test_update_singer.py", "localhost", pgServer.getLocalPort());
    assertEquals("Updated singer 123-456-789 with full name Newfirstname Newlastname\n", output);
  }

  @Test
  public void testGetAlbum() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE albums.id = '987-654-321'"),
            ResultSet.newBuilder()
                .setMetadata(createAlbumsMetadata("albums_"))
                .addRows(
                    createAlbumRow(
                        "123-456-789",
                        "My title",
                        "9423.13",
                        Date.parseDate("2002-10-17"),
                        ByteArray.copyFrom("cover picture"),
                        "123-456-789",
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z"),
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z")))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT tracks.version_id AS tracks_version_id, tracks.created_at AS tracks_created_at, tracks.updated_at AS tracks_updated_at, tracks.id AS tracks_id, tracks.track_number AS tracks_track_number, tracks.title AS tracks_title, tracks.sample_rate AS tracks_sample_rate \n"
                    + "FROM tracks \n"
                    + "WHERE '123-456-789' = tracks.id"),
            ResultSet.newBuilder()
                .setMetadata(createTracksMetadata("tracks_"))
                .addRows(
                    createTrackRow(
                        "123-456-789",
                        1L,
                        "Track 1",
                        6.34324,
                        Timestamp.parseTimestamp("2018-02-28T17:00:00Z"),
                        Timestamp.parseTimestamp("2018-02-01T09:00:00Z")))
                .addRows(
                    createTrackRow(
                        "123-456-789",
                        2L,
                        "Track 2",
                        6.34324,
                        Timestamp.parseTimestamp("2018-02-28T17:00:00Z"),
                        Timestamp.parseTimestamp("2018-02-01T09:00:00Z")))
                .build()));

    String output = execute(SAMPLE_DIR, "test_get_album.py", "localhost", pgServer.getLocalPort());
    assertEquals(
        "albums(id='123-456-789',title='My title',marketing_budget=Decimal('9423.13'),release_date=datetime.date(2002, 10, 17),cover_picture=b'cover picture',singer='123-456-789',created_at='2022-02-21T10:19:18+00:00',updated_at='2022-02-21T10:19:18+00:00')\n"
            + "Tracks:\n"
            + "[tracks(id='123-456-789',track_number=1,title='Track 1',sample_rate=6.34324,created_at='2018-02-28T17:00:00+00:00',updated_at='2018-02-01T09:00:00+00:00'), "
            + "tracks(id='123-456-789',track_number=2,title='Track 2',sample_rate=6.34324,created_at='2018-02-28T17:00:00+00:00',updated_at='2018-02-01T09:00:00+00:00')]\n",
        output);
  }

  @Test
  public void testGetAlbumWithStaleEngine() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
                    + "FROM albums \n"
                    + "WHERE albums.id = '987-654-321'"),
            ResultSet.newBuilder()
                .setMetadata(createAlbumsMetadata("albums_"))
                .addRows(
                    createAlbumRow(
                        "123-456-789",
                        "My title",
                        "9423.13",
                        Date.parseDate("2002-10-17"),
                        ByteArray.copyFrom("cover picture"),
                        "123-456-789",
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z"),
                        Timestamp.parseTimestamp("2022-02-21T10:19:18Z")))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT tracks.version_id AS tracks_version_id, tracks.created_at AS tracks_created_at, tracks.updated_at AS tracks_updated_at, tracks.id AS tracks_id, tracks.track_number AS tracks_track_number, tracks.title AS tracks_title, tracks.sample_rate AS tracks_sample_rate \n"
                    + "FROM tracks \n"
                    + "WHERE '123-456-789' = tracks.id"),
            ResultSet.newBuilder()
                .setMetadata(createTracksMetadata("tracks_"))
                .addRows(
                    createTrackRow(
                        "123-456-789",
                        1L,
                        "Track 1",
                        6.34324,
                        Timestamp.parseTimestamp("2018-02-28T17:00:00Z"),
                        Timestamp.parseTimestamp("2018-02-01T09:00:00Z")))
                .addRows(
                    createTrackRow(
                        "123-456-789",
                        2L,
                        "Track 2",
                        6.34324,
                        Timestamp.parseTimestamp("2018-02-28T17:00:00Z"),
                        Timestamp.parseTimestamp("2018-02-01T09:00:00Z")))
                .build()));

    String output =
        execute(
            SAMPLE_DIR,
            "test_get_album_with_stale_engine.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals("", output);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    ExecuteSqlRequest request = requests.get(1);
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    assertTrue(request.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());
    assertEquals(
        Duration.newBuilder().setSeconds(10L).setNanos(0).build(),
        request.getTransaction().getSingleUse().getReadOnly().getMaxStaleness());
  }

  @Test
  public void testGetTrack() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT tracks.version_id AS tracks_version_id, tracks.created_at AS tracks_created_at, tracks.updated_at AS tracks_updated_at, tracks.id AS tracks_id, tracks.track_number AS tracks_track_number, tracks.title AS tracks_title, tracks.sample_rate AS tracks_sample_rate \n"
                    + "FROM tracks \n"
                    + "WHERE tracks.id = '987-654-321' AND tracks.track_number = 1"),
            ResultSet.newBuilder()
                .setMetadata(createTracksMetadata("tracks_"))
                .addRows(
                    createTrackRow(
                        "987-654-321",
                        1L,
                        "Track 1",
                        6.34324,
                        Timestamp.parseTimestamp("2018-02-28T17:00:00Z"),
                        Timestamp.parseTimestamp("2018-02-01T09:00:00Z")))
                .build()));

    String output = execute(SAMPLE_DIR, "test_get_track.py", "localhost", pgServer.getLocalPort());
    assertEquals(
        "tracks(id='987-654-321',track_number=1,title='Track 1',sample_rate=6.34324,created_at='2018-02-28T17:00:00+00:00',updated_at='2018-02-01T09:00:00+00:00')\n",
        output);
  }

  @Test
  public void testCreateDataModel() throws Exception {
    String checkTableExistsSql =
        "with "
            + EMULATED_PG_CLASS_PREFIX
            + ",\n"
            + "pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + ")\n"
            + "select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where true and relname='%s'";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(checkTableExistsSql, "singers")),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(checkTableExistsSql, "albums")),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(checkTableExistsSql, "tracks")),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(checkTableExistsSql, "venues")),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(String.format(checkTableExistsSql, "concerts")),
            ResultSet.newBuilder().setMetadata(SELECT1_RESULTSET.getMetadata()).build()));
    addDdlResponseToSpannerAdmin();

    String actualOutput =
        execute(SAMPLE_DIR, "test_create_model.py", "localhost", pgServer.getLocalPort());
    String expectedOutput = "Created data model\n";
    assertEquals(expectedOutput, actualOutput);

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(req -> req instanceof UpdateDatabaseDdlRequest)
            .map(req -> (UpdateDatabaseDdlRequest) req)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    assertEquals(5, requests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE singers (\n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tfirst_name VARCHAR(100), \n"
            + "\tlast_name VARCHAR(200), \n"
            + "\tfull_name VARCHAR, \n"
            + "\tactive BOOLEAN, \n"
            + "\tPRIMARY KEY (id)\n"
            + ")",
        requests.get(0).getStatements(0));
    assertEquals(
        "CREATE TABLE venues (\n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tname VARCHAR(200), \n"
            + "\tdescription JSONB, \n"
            + "\tPRIMARY KEY (id)\n"
            + ")",
        requests.get(0).getStatements(1));
    assertEquals(
        "CREATE TABLE albums (\n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\ttitle VARCHAR(200), \n"
            + "\tmarketing_budget NUMERIC, \n"
            + "\trelease_date DATE, \n"
            + "\tcover_picture BYTEA, \n"
            + "\tsinger_id VARCHAR, \n"
            + "\tPRIMARY KEY (id), \n"
            + "\tFOREIGN KEY(singer_id) REFERENCES singers (id)\n"
            + ")",
        requests.get(0).getStatements(2));
    assertEquals(
        "CREATE TABLE concerts (\n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tname VARCHAR(200), \n"
            + "\tvenue_id VARCHAR, \n"
            + "\tsinger_id VARCHAR, \n"
            + "\tstart_time TIMESTAMP WITH TIME ZONE, \n"
            + "\tend_time TIMESTAMP WITH TIME ZONE, \n"
            + "\tPRIMARY KEY (id), \n"
            + "\tFOREIGN KEY(venue_id) REFERENCES venues (id), \n"
            + "\tFOREIGN KEY(singer_id) REFERENCES singers (id)\n"
            + ")",
        requests.get(0).getStatements(3));
    // The 'tracks' table is not generated 100% according to what we would want, but that is because
    // the PostgreSQL SQLAlchemy provider does not understand interleaved tables.
    assertEquals(
        "CREATE TABLE tracks (\n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\ttrack_number INTEGER NOT NULL, \n"
            + "\ttitle VARCHAR, \n"
            + "\tsample_rate FLOAT, \n"
            + "\tPRIMARY KEY (id, track_number), \n"
            + "\tFOREIGN KEY(id) REFERENCES albums (id)\n"
            + ")",
        requests.get(0).getStatements(4));
  }

  @Test
  public void testMetadataReflect() throws Exception {
    String sql =
        "with "
            + EMULATED_PG_CLASS_PREFIX
            + ",\n"
            + "pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + ")\n"
            + "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind in ('r', 'p')";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("relname")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .build()));

    String actualOutput =
        execute(SAMPLE_DIR, "test_metadata_reflect.py", "localhost", pgServer.getLocalPort());
    String expectedOutput = "Reflected current data model\n";
    assertEquals(expectedOutput, actualOutput);
  }

  static ResultSetMetadata createSingersMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .setName(prefix + "version_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "created_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "updated_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "first_name")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "last_name")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "full_name")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                        .setName(prefix + "active")
                        .build())
                .build())
        .build();
  }

  static ListValue createSingerRow(
      String id,
      String firstName,
      String lastName,
      boolean active,
      Timestamp createdAt,
      Timestamp updatedAt) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(firstName).build())
        .addValues(Value.newBuilder().setStringValue(lastName).build())
        .addValues(Value.newBuilder().setStringValue(firstName + " " + lastName).build())
        .addValues(Value.newBuilder().setBoolValue(active).build())
        .build();
  }

  static ResultSetMetadata createAlbumsMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .setName(prefix + "version_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "created_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "updated_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "title")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(
                            Type.newBuilder()
                                .setCode(TypeCode.NUMERIC)
                                .setTypeAnnotation(TypeAnnotationCode.PG_NUMERIC)
                                .build())
                        .setName(prefix + "marketing_budget")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.DATE).build())
                        .setName(prefix + "release_date")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.BYTES).build())
                        .setName(prefix + "cover_picture")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "singer_id")
                        .build())
                .build())
        .build();
  }

  static ListValue createAlbumRow(
      String id,
      String title,
      String marketingBudget,
      Date releaseDate,
      ByteArray coverPicture,
      String singerId,
      Timestamp createdAt,
      Timestamp updatedAt) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(title).build())
        .addValues(Value.newBuilder().setStringValue(marketingBudget).build())
        .addValues(Value.newBuilder().setStringValue(releaseDate.toString()).build())
        .addValues(Value.newBuilder().setStringValue(coverPicture.toBase64()).build())
        .addValues(Value.newBuilder().setStringValue(singerId).build())
        .build();
  }

  static ResultSetMetadata createTracksMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .setName(prefix + "version_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "created_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "updated_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .setName(prefix + "track_number")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "title")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build())
                        .setName(prefix + "sample_rate")
                        .build())
                .build())
        .build();
  }

  static ListValue createTrackRow(
      String id,
      long trackNumber,
      String title,
      double sampleRate,
      Timestamp createdAt,
      Timestamp updatedAt) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(trackNumber)).build())
        .addValues(Value.newBuilder().setStringValue(title).build())
        .addValues(Value.newBuilder().setNumberValue(sampleRate).build())
        .build();
  }

  static ResultSetMetadata createConcertsMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .setName(prefix + "version_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "created_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "updated_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "name")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "venue_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "singer_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "start_time")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "end_time")
                        .build())
                .build())
        .build();
  }

  static ListValue createConcertRow(
      String id,
      String name,
      String venueId,
      String singerId,
      Timestamp startTime,
      Timestamp endTime,
      Timestamp createdAt,
      Timestamp updatedAt) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(name).build())
        .addValues(Value.newBuilder().setStringValue(venueId).build())
        .addValues(Value.newBuilder().setStringValue(singerId).build())
        .addValues(Value.newBuilder().setStringValue(startTime.toString()).build())
        .addValues(Value.newBuilder().setStringValue(endTime.toString()).build())
        .build();
  }

  static ResultSetMetadata createVenuesMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .setName(prefix + "version_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "created_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                        .setName(prefix + "updated_at")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "name")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "description")
                        .build())
                .build())
        .build();
  }

  static ListValue createVenueRow(
      String id, String name, String description, Timestamp createdAt, Timestamp updatedAt) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(name).build())
        .addValues(Value.newBuilder().setStringValue(description).build())
        .build();
  }
}
