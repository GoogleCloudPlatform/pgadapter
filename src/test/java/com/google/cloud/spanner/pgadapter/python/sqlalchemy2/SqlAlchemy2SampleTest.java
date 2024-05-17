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

package com.google.cloud.spanner.pgadapter.python.sqlalchemy2;

import static com.google.cloud.spanner.pgadapter.python.sqlalchemy2.SqlAlchemy2BasicsTest.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PythonTest.class)
public class SqlAlchemy2SampleTest extends AbstractMockServerTest {
  private static final String SAMPLE_DIR = "./samples/python/sqlalchemy2-sample";

  @BeforeClass
  public static void setupBaseResults() throws Exception {
    SqlAlchemy2BasicsTest.setupBaseResults(SAMPLE_DIR);
  }

  @Test
  public void testDeleteAlbum() throws Exception {
    String sql =
        "SELECT albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, "
            + "albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, "
            + "albums.singer_id AS albums_singer_id, albums.id AS albums_id, albums.version_id AS albums_version_id, "
            + "albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at \n"
            + "FROM albums \n"
            + "WHERE albums.id = $1::VARCHAR";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAlbumsMetadata("albums_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("123-456-789").build(),
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
    String deleteSql =
        "DELETE FROM albums WHERE albums.id = $1::VARCHAR AND albums.version_id = $2::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(deleteSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(deleteSql).bind("p1").to("123-456-789").bind("p2").to(1L).build(),
            1L));

    String output =
        execute(SAMPLE_DIR, "test_delete_album.py", "localhost", pgServer.getLocalPort());
    assertEquals("\n" + "Deleted album with id 123-456-789\n", output);
  }

  @Test
  public void testAlbumsWithTitleFirstCharEqualToSingerName() throws Exception {
    String sql =
        "SELECT albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id, albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at \n"
            + "FROM albums JOIN singers ON singers.id = albums.singer_id \n"
            + "WHERE lower(SUBSTRING(albums.title FROM $1::INTEGER FOR $2::INTEGER)) = lower(SUBSTRING(singers.first_name FROM $3::INTEGER FOR $4::INTEGER)) OR lower(SUBSTRING(albums.title FROM $5::INTEGER FOR $6::INTEGER)) = lower(SUBSTRING(singers.last_name FROM $7::INTEGER FOR $8::INTEGER))";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(1L)
                .bind("p4")
                .to(1L)
                .bind("p5")
                .to(1L)
                .bind("p6")
                .to(1L)
                .bind("p7")
                .to(1L)
                .bind("p8")
                .to(1L)
                .build(),
            ResultSet.newBuilder().setMetadata(createAlbumsMetadata("albums_")).build()));

    String output =
        execute(
            SAMPLE_DIR,
            "test_print_albums_first_character_of_title_equal_to_first_or_last_name.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals(
        "\n"
            + "Searching for albums that have a title that starts with the same character as the first or last name of the singer\n",
        output);
  }

  @Test
  public void testPrintVenuesWithCapacityAtLeast5000() throws Exception {
    String sql =
        "SELECT venues.name AS venues_name, venues.description AS venues_description, venues.id AS venues_id, venues.version_id AS venues_version_id, venues.created_at AS venues_created_at, venues.updated_at AS venues_updated_at \n"
            + "FROM venues \n"
            + "WHERE CAST((venues.description ->> $1::TEXT) AS INTEGER) >= $2::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createVenuesMetadata("venues_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(TypeCode.STRING, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("Capacity").bind("p2").to(5000L).build(),
            ResultSet.newBuilder()
                .setMetadata(createVenuesMetadata("venues_"))
                .addRows(
                    createVenueRow(
                        "123",
                        "Avenue Park",
                        "{\"Capacity\": 5000, \"Location\": \"New York\"}",
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z"),
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z")))
                .build()));

    String output =
        execute(
            SAMPLE_DIR,
            "test_print_venues_with_capacity_at_least_5000.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals(
        "\n"
            + "Searching for venues that have a capacity of at least 5,000\n"
            + "  'Avenue Park' has capacity 5000\n",
        output);
  }

  @Test
  public void testSingersWithLimitAndOffset() throws Exception {
    String sql =
        "SELECT singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active, singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at \n"
            + "FROM singers ORDER BY singers.last_name \n"
            + " LIMIT $1::INTEGER OFFSET $2::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(5L).bind("p2").to(3L).build(),
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
    String sql =
        "SELECT albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id, albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at \n"
            + "FROM albums \n"
            + "WHERE albums.release_date < $1::DATE";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(Date.parseDate("1980-01-01")).build(),
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

    String sql =
        "SELECT concerts.name AS concerts_name, concerts.venue_id AS concerts_venue_id, concerts.singer_id AS concerts_singer_id, concerts.start_time AS concerts_start_time, concerts.end_time AS concerts_end_time, concerts.id AS concerts_id, concerts.version_id AS concerts_version_id, concerts.created_at AS concerts_created_at, concerts.updated_at AS concerts_updated_at, venues_1.name AS venues_1_name, venues_1.description AS venues_1_description, venues_1.id AS venues_1_id, venues_1.version_id AS venues_1_version_id, venues_1.created_at AS venues_1_created_at, venues_1.updated_at AS venues_1_updated_at, singers_1.first_name AS singers_1_first_name, singers_1.last_name AS singers_1_last_name, singers_1.full_name AS singers_1_full_name, singers_1.active AS singers_1_active, singers_1.id AS singers_1_id, singers_1.version_id AS singers_1_version_id, singers_1.created_at AS singers_1_created_at, singers_1.updated_at AS singers_1_updated_at \n"
            + "FROM concerts "
            + "LEFT OUTER JOIN venues AS venues_1 ON venues_1.id = concerts.venue_id "
            + "LEFT OUTER JOIN singers AS singers_1 ON singers_1.id = concerts.singer_id "
            + "ORDER BY concerts.start_time";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
    String ticketSalesSql =
        "SELECT ticket_sales.concert_id AS ticket_sales_concert_id, ticket_sales.customer_name AS ticket_sales_customer_name, ticket_sales.price AS ticket_sales_price, ticket_sales.seats AS ticket_sales_seats, ticket_sales.id AS ticket_sales_id, ticket_sales.version_id AS ticket_sales_version_id, ticket_sales.created_at AS ticket_sales_created_at, ticket_sales.updated_at AS ticket_sales_updated_at \n"
            + "FROM ticket_sales \n"
            + "WHERE $1::VARCHAR = ticket_sales.concert_id";
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of(ticketSalesSql),
            ResultSet.newBuilder()
                .setMetadata(createTicketSalesMetadata("ticket_sales_"))
                .addRows(
                    createTicketSaleRow(
                        1L,
                        "c1",
                        "Alice",
                        "99.99",
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z"),
                        Timestamp.parseTimestamp("2022-12-02T17:30:00Z")))
                .build()));

    String output =
        execute(SAMPLE_DIR, "test_print_concerts.py", "localhost", pgServer.getLocalPort());
    assertEquals(
        "\n"
            + "Concert 'Avenue Park Open' starting at 2023-02-02T01:00:00+00:00 with Pete Allison will be held at Avenue Park\n"
            + "  Ticket sold to Alice for seats ['A10']\n",
        output);
  }

  @Test
  public void testCreateVenueAndConcertInTransaction() throws Exception {
    String sql =
        "SELECT singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active, singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at \n"
            + "FROM singers \n"
            + " LIMIT $1::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
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
    String insertVenueSql =
        "INSERT INTO venues (name, description, id, version_id, created_at, updated_at) "
            + "VALUES ($1::VARCHAR(200), $2::JSONB, $3::VARCHAR, $4::INTEGER, $5::TIMESTAMP WITH TIME ZONE, $6::TIMESTAMP WITH TIME ZONE)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertVenueSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.STRING,
                            TypeCode.JSON,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.TIMESTAMP,
                            TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.update(
            Statement.of(
                "INSERT INTO venues (name, description, id, version_id, created_at, updated_at) "),
            1L));
    String insertConcertSql =
        "INSERT INTO concerts (name, venue_id, singer_id, start_time, end_time, id, version_id, created_at, updated_at) "
            + "VALUES ($1::VARCHAR(200), $2::VARCHAR, $3::VARCHAR, $4::TIMESTAMP WITH TIME ZONE, $5::TIMESTAMP WITH TIME ZONE, $6::VARCHAR, $7::INTEGER, $8::TIMESTAMP WITH TIME ZONE, $9::TIMESTAMP WITH TIME ZONE)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertConcertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.TIMESTAMP,
                            TypeCode.TIMESTAMP,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.TIMESTAMP,
                            TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.update(
            Statement.of(
                "INSERT INTO concerts (name, venue_id, singer_id, start_time, end_time, id, version_id, created_at, updated_at) "),
            1L));
    String insertTicketSaleSql =
        "INSERT INTO ticket_sales (concert_id, customer_name, price, seats, version_id, created_at, updated_at) "
            + "VALUES ($1::VARCHAR, $2::VARCHAR, $3, $4, $5::INTEGER, $6::TIMESTAMP WITH TIME ZONE, $7::TIMESTAMP WITH TIME ZONE) "
            + "RETURNING ticket_sales.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertTicketSaleSql),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("id")
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
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p3")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.NUMERIC).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p4")
                                        .setType(
                                            Type.newBuilder()
                                                .setCode(TypeCode.ARRAY)
                                                .setArrayElementType(
                                                    Type.newBuilder()
                                                        .setCode(TypeCode.STRING)
                                                        .build())
                                                .build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p5")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p6")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p7")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build())
                        .build())
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO ticket_sales (concert_id, customer_name, price, seats, version_id, created_at, updated_at) "),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("id")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .build()));

    String output =
        execute(
            SAMPLE_DIR,
            "test_create_venue_and_concert_in_transaction.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals("\nCreated Venue and Concert\n", output);
  }

  @Ignore(
      "Skipped as SQLAlchemy requires the server to return the exact ID that was inserted, "
          + "which we do not know beforehand")
  @Test
  public void testCreateRandomSingersAndAlbums() throws Exception {
    String insertSingersSql =
        "INSERT INTO singers (first_name, last_name, active, id, version_id, created_at, updated_at) VALUES "
            + "($1::VARCHAR, $2::VARCHAR, $3, $4::VARCHAR, $5::INTEGER, $6::TIMESTAMP WITH TIME ZONE, $7::TIMESTAMP WITH TIME ZONE), "
            + "($8::VARCHAR, $9::VARCHAR, $10, $11::VARCHAR, $12::INTEGER, $13::TIMESTAMP WITH TIME ZONE, $14::TIMESTAMP WITH TIME ZONE), "
            + "($15::VARCHAR, $16::VARCHAR, $17, $18::VARCHAR, $19::INTEGER, $20::TIMESTAMP WITH TIME ZONE, $21::TIMESTAMP WITH TIME ZONE), "
            + "($22::VARCHAR, $23::VARCHAR, $24, $25::VARCHAR, $26::INTEGER, $27::TIMESTAMP WITH TIME ZONE, $28::TIMESTAMP WITH TIME ZONE), "
            + "($29::VARCHAR, $30::VARCHAR, $31, $32::VARCHAR, $33::INTEGER, $34::TIMESTAMP WITH TIME ZONE, $35::TIMESTAMP WITH TIME ZONE) "
            + "RETURNING singers.full_name, singers.id";

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSingersSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                            ImmutableList.of(
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.BOOL,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP,
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.BOOL,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP,
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.BOOL,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP,
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.BOOL,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP,
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.BOOL,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP))
                        .toBuilder()
                        .setRowType(
                            createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING))
                                .getRowType()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO singers (first_name, last_name, active, id, version_id, created_at, updated_at) VALUES"),
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
                                .addFields(
                                    Field.newBuilder()
                                        .setName("id")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build()))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .addValues(Value.newBuilder().setStringValue("3").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .addValues(Value.newBuilder().setStringValue("4").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("(unknown)").build())
                        .addValues(Value.newBuilder().setStringValue("5").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(5L).build())
                .build()));
    String insertAlbumSql =
        "INSERT INTO albums (title, marketing_budget, release_date, cover_picture, singer_id, id, version_id, created_at, updated_at) "
            + "VALUES ($1::VARCHAR, $2, $3::DATE, $4, $5::VARCHAR, $6::VARCHAR, $7::INTEGER, $8::TIMESTAMP WITH TIME ZONE, $9::TIMESTAMP WITH TIME ZONE)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertAlbumSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.STRING,
                            TypeCode.NUMERIC,
                            TypeCode.DATE,
                            TypeCode.BYTES,
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.TIMESTAMP,
                            TypeCode.TIMESTAMP)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.update(
            Statement.of(
                "INSERT INTO albums (title, marketing_budget, release_date, cover_picture, singer_id, id, version_id, created_at, updated_at) VALUES"),
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
    String sql =
        "SELECT singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active, singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at \n"
            + "FROM singers ORDER BY singers.last_name";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
    String getAlbumsSql =
        "SELECT albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id, albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at \n"
            + "FROM albums \n"
            + "WHERE $1::VARCHAR = albums.singer_id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getAlbumsSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAlbumsMetadata("albums_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(getAlbumsSql).bind("p1").to("b2").build(),
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
            Statement.newBuilder(getAlbumsSql).bind("p1").to("a1").build(),
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
            Statement.newBuilder(getAlbumsSql).bind("p1").to("c3").build(),
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
    assertEquals(7, requests.size());
  }

  @Test
  public void testGetSinger() throws Exception {
    String getSingerSql =
        "SELECT singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active, singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at \n"
            + "FROM singers \n"
            + "WHERE singers.id = $1::VARCHAR";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getSingerSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createSingersMetadata("singers_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(getSingerSql).bind("p1").to("123-456-789").build(),
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
    String getAlbumsSql =
        "SELECT albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id, albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at \n"
            + "FROM albums \n"
            + "WHERE $1::VARCHAR = albums.singer_id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getAlbumsSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAlbumsMetadata("albums_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(getAlbumsSql).bind("p1").to("123-456-789").build(),
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
    String insertSingerSql =
        "INSERT INTO singers (first_name, last_name, active, id, version_id, created_at, updated_at) "
            + "VALUES ($1::VARCHAR, $2::VARCHAR, $3, $4::VARCHAR, $5::INTEGER, $6::TIMESTAMP WITH TIME ZONE, $7::TIMESTAMP WITH TIME ZONE) RETURNING singers.full_name";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSingerSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                            ImmutableList.of(
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.BOOL,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.TIMESTAMP,
                                TypeCode.TIMESTAMP))
                        .toBuilder()
                        .setRowType(createMetadata(ImmutableList.of(TypeCode.STRING)).getRowType())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSingerSql)
                .bind("p1")
                .to("Myfirstname")
                .bind("p2")
                .to("Mylastname")
                .bind("p3")
                .to(true)
                .bind("p4")
                .to("123-456-789")
                .bind("p5")
                .to(1L)
                .bind("p6")
                .to(Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"))
                .bind("p7")
                .to((Timestamp) null)
                .build(),
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
    String getSingerSql =
        "SELECT singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active, singers.id AS singers_id, singers.version_id AS singers_version_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at \n"
            + "FROM singers \n"
            + "WHERE singers.id = $1::VARCHAR";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getSingerSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createSingersMetadata("singers_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(getSingerSql).bind("p1").to("123-456-789").build(),
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
    String updateSql =
        "UPDATE singers SET "
            + "first_name=$1::VARCHAR, last_name=$2::VARCHAR, version_id=$3::INTEGER, "
            + "updated_at=$4::TIMESTAMP WITH TIME ZONE "
            + "WHERE singers.id = $5::VARCHAR "
            + "AND singers.version_id = $6::INTEGER RETURNING singers.full_name";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                            ImmutableList.of(
                                TypeCode.STRING,
                                TypeCode.STRING,
                                TypeCode.INT64,
                                TypeCode.TIMESTAMP,
                                TypeCode.STRING,
                                TypeCode.INT64))
                        .toBuilder()
                        .setRowType(createMetadata(ImmutableList.of(TypeCode.STRING)).getRowType()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    // We have to use a partial SQL string here, as we don't know exactly what updated_at timestamp
    // will be used by SQLAlchemy.
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
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
    String getAlbumSql =
        "SELECT albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id, albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at \n"
            + "FROM albums \n"
            + "WHERE albums.id = $1::VARCHAR";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getAlbumSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAlbumsMetadata("albums_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(getAlbumSql).bind("p1").to("987-654-321").build(),
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
    String getTrackSql =
        "SELECT tracks.id AS tracks_id, tracks.track_number AS tracks_track_number, tracks.title AS tracks_title, tracks.sample_rate AS tracks_sample_rate, tracks.version_id AS tracks_version_id, tracks.created_at AS tracks_created_at, tracks.updated_at AS tracks_updated_at \n"
            + "FROM tracks \n"
            + "WHERE $1::VARCHAR = tracks.id";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getTrackSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createTracksMetadata("tracks_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(getTrackSql).bind("p1").to("123-456-789").build(),
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
    String getAlbumSql =
        "SELECT albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id, albums.id AS albums_id, albums.version_id AS albums_version_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at \n"
            + "FROM albums \n"
            + "WHERE albums.id = $1::VARCHAR";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getAlbumSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAlbumsMetadata("albums_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(getAlbumSql).bind("p1").to("987-654-321").build(),
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

    String output =
        execute(
            SAMPLE_DIR,
            "test_get_album_with_stale_engine.py",
            "localhost",
            pgServer.getLocalPort());
    assertEquals("", output);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(4, requests.size());
    for (int index = 2; index < 4; index++) {
      ExecuteSqlRequest request = requests.get(index);
      assertTrue(request.getTransaction().hasSingleUse());
      assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
      assertTrue(request.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());
      assertEquals(
          Duration.newBuilder().setSeconds(10L).setNanos(0).build(),
          request.getTransaction().getSingleUse().getReadOnly().getMaxStaleness());
    }
  }

  @Test
  public void testGetTrack() throws Exception {
    String sql =
        "SELECT tracks.id AS tracks_id, tracks.track_number AS tracks_track_number, tracks.title AS tracks_title, tracks.sample_rate AS tracks_sample_rate, tracks.version_id AS tracks_version_id, tracks.created_at AS tracks_created_at, tracks.updated_at AS tracks_updated_at \n"
            + "FROM tracks \n"
            + "WHERE tracks.id = $1::VARCHAR AND tracks.track_number = $2::INTEGER";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createTracksMetadata("tracks_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(TypeCode.STRING, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("987-654-321").bind("p2").to(1L).build(),
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

  @Ignore("Requires support for array casting/coercion")
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
            + "SELECT pg_class.relname \n"
            + "FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace \n"
            + "WHERE pg_class.relname = $1::VARCHAR "
            + "AND pg_class.relkind = ANY (ARRAY[$2::VARCHAR, $3::VARCHAR, $4::VARCHAR, $5::VARCHAR, $6::VARCHAR]) "
            + "AND true AND pg_namespace.nspname != $7::VARCHAR";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(checkTableExistsSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(ImmutableList.of(TypeCode.STRING))
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.STRING,
                                        TypeCode.STRING,
                                        TypeCode.STRING,
                                        TypeCode.STRING,
                                        TypeCode.STRING,
                                        TypeCode.STRING,
                                        TypeCode.STRING))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(checkTableExistsSql)
                .bind("p1")
                .to("singers")
                .bind("p2")
                .to("r")
                .bind("p3")
                .to("p")
                .bind("p4")
                .to("f")
                .bind("p5")
                .to("v")
                .bind("p6")
                .to("m")
                .bind("p7")
                .to("pg_catalog")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(checkTableExistsSql)
                .bind("p1")
                .to("albums")
                .bind("p2")
                .to("r")
                .bind("p3")
                .to("p")
                .bind("p4")
                .to("f")
                .bind("p5")
                .to("v")
                .bind("p6")
                .to("m")
                .bind("p7")
                .to("pg_catalog")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(checkTableExistsSql)
                .bind("p1")
                .to("tracks")
                .bind("p2")
                .to("r")
                .bind("p3")
                .to("p")
                .bind("p4")
                .to("f")
                .bind("p5")
                .to("v")
                .bind("p6")
                .to("m")
                .bind("p7")
                .to("pg_catalog")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(checkTableExistsSql)
                .bind("p1")
                .to("venues")
                .bind("p2")
                .to("r")
                .bind("p3")
                .to("p")
                .bind("p4")
                .to("f")
                .bind("p5")
                .to("v")
                .bind("p6")
                .to("m")
                .bind("p7")
                .to("pg_catalog")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(checkTableExistsSql)
                .bind("p1")
                .to("concerts")
                .bind("p2")
                .to("r")
                .bind("p3")
                .to("p")
                .bind("p4")
                .to("f")
                .bind("p5")
                .to("v")
                .bind("p6")
                .to("m")
                .bind("p7")
                .to("pg_catalog")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(checkTableExistsSql)
                .bind("p1")
                .to("ticket_sales")
                .bind("p2")
                .to("r")
                .bind("p3")
                .to("p")
                .bind("p4")
                .to("f")
                .bind("p5")
                .to("v")
                .bind("p6")
                .to("m")
                .bind("p7")
                .to("pg_catalog")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
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
    assertEquals(6, requests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE singers (\n"
            + "\tfirst_name VARCHAR(100), \n"
            + "\tlast_name VARCHAR(200), \n"
            + "\tfull_name VARCHAR, \n"
            + "\tactive BOOLEAN, \n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tPRIMARY KEY (id)\n"
            + ")",
        requests.get(0).getStatements(0));
    assertEquals(
        "CREATE TABLE venues (\n"
            + "\tname VARCHAR(200), \n"
            + "\tdescription JSONB, \n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tPRIMARY KEY (id)\n"
            + ")",
        requests.get(0).getStatements(1));
    assertEquals(
        "CREATE TABLE albums (\n"
            + "\ttitle VARCHAR(200), \n"
            + "\tmarketing_budget NUMERIC, \n"
            + "\trelease_date DATE, \n"
            + "\tcover_picture BYTEA, \n"
            + "\tsinger_id VARCHAR, \n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tPRIMARY KEY (id), \n"
            + "\tFOREIGN KEY(singer_id) REFERENCES singers (id)\n"
            + ")",
        requests.get(0).getStatements(2));
    assertEquals(
        "CREATE TABLE concerts (\n"
            + "\tname VARCHAR(200), \n"
            + "\tvenue_id VARCHAR, \n"
            + "\tsinger_id VARCHAR, \n"
            + "\tstart_time TIMESTAMP WITH TIME ZONE, \n"
            + "\tend_time TIMESTAMP WITH TIME ZONE, \n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tPRIMARY KEY (id), \n"
            + "\tFOREIGN KEY(venue_id) REFERENCES venues (id), \n"
            + "\tFOREIGN KEY(singer_id) REFERENCES singers (id)\n"
            + ")",
        requests.get(0).getStatements(3));
    // The 'tracks' table is not generated 100% according to what we would want, but that is because
    // the PostgreSQL SQLAlchemy provider does not understand interleaved tables.
    assertEquals(
        "CREATE TABLE tracks (\n"
            + "\tid VARCHAR NOT NULL, \n"
            + "\ttrack_number INTEGER NOT NULL, \n"
            + "\ttitle VARCHAR, \n"
            + "\tsample_rate FLOAT, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tPRIMARY KEY (id, track_number), \n"
            + "\tFOREIGN KEY(id) REFERENCES albums (id)\n"
            + ")",
        requests.get(0).getStatements(4));
    assertEquals(
        "CREATE TABLE ticket_sales (\n"
            + "\tconcert_id VARCHAR, \n"
            + "\tcustomer_name VARCHAR, \n"
            + "\tprice NUMERIC, \n"
            + "\tseats VARCHAR[], \n"
            + "\tid SERIAL NOT NULL, \n"
            + "\tversion_id INTEGER NOT NULL, \n"
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tPRIMARY KEY (id), \n"
            + "\tFOREIGN KEY(concert_id) REFERENCES concerts (id)\n"
            + ")",
        requests.get(0).getStatements(5));
  }

  @Ignore("Uses too much pg_catalog tables")
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
            + "SELECT pg_catalog.pg_class.relname \n"
            + "FROM pg_class JOIN pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace \n"
            + "WHERE pg_catalog.pg_class.relkind = ANY (ARRAY[$1::VARCHAR, $2::VARCHAR]) AND pg_catalog.pg_class.relpersistence != $3::VARCHAR AND true COLLATE \"C\"";

    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("relname")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .setUndeclaredParameters(
                createParameterTypesMetadata(
                        ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getUndeclaredParameters())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ResultSet.newBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to("r")
                .bind("p2")
                .to("p")
                .bind("p3")
                .to("t")
                .build(),
            ResultSet.newBuilder().setMetadata(metadata).build()));

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
        .addValues(Value.newBuilder().setStringValue(firstName).build())
        .addValues(Value.newBuilder().setStringValue(lastName).build())
        .addValues(Value.newBuilder().setStringValue(firstName + " " + lastName).build())
        .addValues(Value.newBuilder().setBoolValue(active).build())
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .build();
  }

  static ResultSetMetadata createAlbumsMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
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
        .addValues(Value.newBuilder().setStringValue(title).build())
        .addValues(Value.newBuilder().setStringValue(marketingBudget).build())
        .addValues(Value.newBuilder().setStringValue(releaseDate.toString()).build())
        .addValues(Value.newBuilder().setStringValue(coverPicture.toBase64()).build())
        .addValues(Value.newBuilder().setStringValue(singerId).build())
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .build();
  }

  static ResultSetMetadata createTracksMetadata(String prefix) {
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
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(trackNumber)).build())
        .addValues(Value.newBuilder().setStringValue(title).build())
        .addValues(Value.newBuilder().setNumberValue(sampleRate).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .build();
  }

  static ResultSetMetadata createTicketSalesMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "concert_id")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "customer_name")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(
                            Type.newBuilder()
                                .setCode(TypeCode.NUMERIC)
                                .setTypeAnnotation(TypeAnnotationCode.PG_NUMERIC)
                                .build())
                        .setName(prefix + "price")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(
                            Type.newBuilder()
                                .setCode(TypeCode.ARRAY)
                                .setArrayElementType(
                                    Type.newBuilder().setCode(TypeCode.STRING).build())
                                .build())
                        .setName(prefix + "seats")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
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
                .build())
        .build();
  }

  static ListValue createTicketSaleRow(
      long id,
      String concertId,
      String customerName,
      String price,
      Timestamp createdAt,
      Timestamp updatedAt) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(concertId).build())
        .addValues(Value.newBuilder().setStringValue(customerName).build())
        .addValues(Value.newBuilder().setStringValue(price).build())
        .addValues(
            Value.newBuilder()
                .setListValue(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("A10").build())
                        .build())
                .build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(id)).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .build();
  }

  static ResultSetMetadata createConcertsMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
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
        .addValues(Value.newBuilder().setStringValue(name).build())
        .addValues(Value.newBuilder().setStringValue(venueId).build())
        .addValues(Value.newBuilder().setStringValue(singerId).build())
        .addValues(Value.newBuilder().setStringValue(startTime.toString()).build())
        .addValues(Value.newBuilder().setStringValue(endTime.toString()).build())
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .build();
  }

  static ResultSetMetadata createVenuesMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "name")
                        .build())
                .addFields(
                    Field.newBuilder()
                        .setType(
                            Type.newBuilder()
                                .setCode(TypeCode.JSON)
                                .setTypeAnnotation(TypeAnnotationCode.PG_JSONB)
                                .build())
                        .setName(prefix + "description")
                        .build())
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
                .build())
        .build();
  }

  static ListValue createVenueRow(
      String id, String name, String description, Timestamp createdAt, Timestamp updatedAt) {
    return ListValue.newBuilder()
        .addValues(Value.newBuilder().setStringValue(name).build())
        .addValues(Value.newBuilder().setStringValue(description).build())
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue("1").build())
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .build();
  }
}
