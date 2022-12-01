package com.google.cloud.spanner.pgadapter.python.sqlalchemy;

import static com.google.cloud.spanner.pgadapter.python.sqlalchemy.SqlAlchemyBasicsTest.execute;
import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
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
  public static void setupBaseResults() {
    SqlAlchemyBasicsTest.setupBaseResults();
  }

  @Test
  public void testGetSinger() throws IOException, InterruptedException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT singers.id AS singers_id, singers.created_at AS singers_created_at, singers.updated_at AS singers_updated_at, singers.first_name AS singers_first_name, singers.last_name AS singers_last_name, singers.full_name AS singers_full_name, singers.active AS singers_active \n"
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
                "SELECT albums.id AS albums_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
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
        "singers(id='123-456-789',first_name='Myfirstname',last_name='Mylastname',active=True,"
            + "created_at=datetime.datetime(2022, 2, 21, 10, 19, 18, tzinfo=datetime.timezone.utc),"
            + "updated_at=datetime.datetime(2022, 2, 21, 10, 19, 18, tzinfo=datetime.timezone.utc))\n"
            + "Albums:\n"
            + "[albums(id='987-654-321',title='My title',marketing_budget=Decimal('9423.13'),"
            + "release_date=datetime.date(2002, 10, 17),cover_picture=b'cover picture',"
            + "singer='123-456-789',created_at=datetime.datetime(2022, 2, 21, 10, 19, 18, tzinfo=datetime.timezone.utc),updated_at=datetime.datetime(2022, 2, 21, 10, 19, 18, tzinfo=datetime.timezone.utc))]\n",
        output);
  }

  @Test
  public void testAddSinger() throws IOException, InterruptedException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO singers (id, created_at, updated_at, first_name, last_name, active) "
                    + "VALUES ('123-456-789', ('2011-11-04T00:05:23.123456+00:00'::timestamptz), NULL, 'Myfirstname', 'Mylastname', true) "
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
  public void testUpdateSinger() throws IOException, InterruptedException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "INSERT INTO singers (id, created_at, updated_at, first_name, last_name, active) "
                    + "VALUES ('123-456-789', ('2011-11-04T00:05:23.123456+00:00'::timestamptz), NULL, 'Myfirstname', 'Mylastname', true) "
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

    String output =
        execute(SAMPLE_DIR, "test_update_singer.py", "localhost", pgServer.getLocalPort());
    assertEquals("Added singer 123-456-789 with full name Myfirstname Mylastname\n", output);
  }

  @Test
  public void testGetAlbum() throws IOException, InterruptedException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT albums.id AS albums_id, albums.created_at AS albums_created_at, albums.updated_at AS albums_updated_at, albums.title AS albums_title, albums.marketing_budget AS albums_marketing_budget, albums.release_date AS albums_release_date, albums.cover_picture AS albums_cover_picture, albums.singer_id AS albums_singer_id \n"
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
                "SELECT tracks.created_at AS tracks_created_at, tracks.updated_at AS tracks_updated_at, tracks.id AS tracks_id, tracks.track_number AS tracks_track_number, tracks.title AS tracks_title, tracks.sample_rate AS tracks_sample_rate \n"
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
        "albums(id='123-456-789',title='My title',marketing_budget=Decimal('9423.13'),release_date=datetime.date(2002, 10, 17),cover_picture=b'cover picture',singer='123-456-789',created_at=datetime.datetime(2022, 2, 21, 10, 19, 18, tzinfo=datetime.timezone.utc),updated_at=datetime.datetime(2022, 2, 21, 10, 19, 18, tzinfo=datetime.timezone.utc))\n"
            + "Tracks:\n"
            + "[tracks(id='123-456-789',track_number=1,title='Track 1',sample_rate=6.34324,created_at=datetime.datetime(2018, 2, 28, 17, 0, tzinfo=datetime.timezone.utc),updated_at=datetime.datetime(2018, 2, 1, 9, 0, tzinfo=datetime.timezone.utc)),"
            + " tracks(id='123-456-789',track_number=2,title='Track 2',sample_rate=6.34324,created_at=datetime.datetime(2018, 2, 28, 17, 0, tzinfo=datetime.timezone.utc),updated_at=datetime.datetime(2018, 2, 1, 9, 0, tzinfo=datetime.timezone.utc))]\n",
        output);
  }

  @Test
  public void testCreateDataModel() throws Exception {
    String checkTableExistsSql =
        "with pg_class as (\n"
            + "  select\n"
            + "  -1 as oid,\n"
            + "  table_name as relname,\n"
            + "  case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "  0 as reltype,\n"
            + "  0 as reloftype,\n"
            + "  0 as relowner,\n"
            + "  1 as relam,\n"
            + "  0 as relfilenode,\n"
            + "  0 as reltablespace,\n"
            + "  0 as relpages,\n"
            + "  0.0::float8 as reltuples,\n"
            + "  0 as relallvisible,\n"
            + "  0 as reltoastrelid,\n"
            + "  false as relhasindex,\n"
            + "  false as relisshared,\n"
            + "  'p' as relpersistence,\n"
            + "  'r' as relkind,\n"
            + "  count(*) as relnatts,\n"
            + "  0 as relchecks,\n"
            + "  false as relhasrules,\n"
            + "  false as relhastriggers,\n"
            + "  false as relhassubclass,\n"
            + "  false as relrowsecurity,\n"
            + "  false as relforcerowsecurity,\n"
            + "  true as relispopulated,\n"
            + "  'n' as relreplident,\n"
            + "  false as relispartition,\n"
            + "  0 as relrewrite,\n"
            + "  0 as relfrozenxid,\n"
            + "  0 as relminmxid,\n"
            + "  '{}'::bigint[] as relacl,\n"
            + "  '{}'::text[] as reloptions,\n"
            + "  0 as relpartbound\n"
            + "from information_schema.tables t\n"
            + "inner join information_schema.columns using (table_catalog, table_schema, table_name)\n"
            + "group by t.table_name, t.table_schema\n"
            + "union all\n"
            + "select\n"
            + "    -1 as oid,\n"
            + "    i.index_name as relname,\n"
            + "    case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "    0 as reltype,\n"
            + "    0 as reloftype,\n"
            + "    0 as relowner,\n"
            + "    1 as relam,\n"
            + "    0 as relfilenode,\n"
            + "    0 as reltablespace,\n"
            + "    0 as relpages,\n"
            + "    0.0::float8 as reltuples,\n"
            + "    0 as relallvisible,\n"
            + "    0 as reltoastrelid,\n"
            + "    false as relhasindex,\n"
            + "    false as relisshared,\n"
            + "    'p' as relpersistence,\n"
            + "    'r' as relkind,\n"
            + "    count(*) as relnatts,\n"
            + "    0 as relchecks,\n"
            + "    false as relhasrules,\n"
            + "    false as relhastriggers,\n"
            + "    false as relhassubclass,\n"
            + "    false as relrowsecurity,\n"
            + "    false as relforcerowsecurity,\n"
            + "    true as relispopulated,\n"
            + "    'n' as relreplident,\n"
            + "    false as relispartition,\n"
            + "    0 as relrewrite,\n"
            + "    0 as relfrozenxid,\n"
            + "    0 as relminmxid,\n"
            + "    '{}'::bigint[] as relacl,\n"
            + "    '{}'::text[] as reloptions,\n"
            + "    0 as relpartbound\n"
            + "from information_schema.indexes i\n"
            + "inner join information_schema.index_columns using (table_catalog, table_schema, table_name)\n"
            + "group by i.index_name, i.table_schema\n"
            + "),\n"
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
            + "\tcreated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tupdated_at TIMESTAMP WITH TIME ZONE, \n"
            + "\tname VARCHAR(200), \n"
            + "\tdescription VARCHAR, \n"
            + "\tPRIMARY KEY (id)\n"
            + ")",
        requests.get(0).getStatements(1));
    assertEquals(
        "CREATE TABLE albums (\n"
            + "\tid VARCHAR NOT NULL, \n"
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

  static ResultSetMetadata createSingersMetadata(String prefix) {
    return ResultSetMetadata.newBuilder()
        .setRowType(
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .setName(prefix + "singers_id")
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
                        .setName(prefix + "albums_id")
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
                        .setName(prefix + "tracks_id")
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
        .addValues(Value.newBuilder().setStringValue(createdAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(updatedAt.toString()).build())
        .addValues(Value.newBuilder().setStringValue(id).build())
        .addValues(Value.newBuilder().setStringValue(String.valueOf(trackNumber)).build())
        .addValues(Value.newBuilder().setStringValue(title).build())
        .addValues(Value.newBuilder().setNumberValue(sampleRate).build())
        .build();
  }
}
