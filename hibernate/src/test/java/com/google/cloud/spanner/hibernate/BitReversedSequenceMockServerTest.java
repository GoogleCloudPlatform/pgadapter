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

package com.google.cloud.spanner.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.PostgreSQLDialect;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BitReversedSequenceMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void addGetKeywordsResult() {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "select 'abort,access,aggregate,also,analyse,analyze,backward,bit,cache,checkpoint,class,cluster,comment,concurrently,connection,conversion,copy,csv,database,delimiter,delimiters,disable,do,enable,encoding,encrypted,exclusive,explain,force,forward,freeze,greatest,handler,header,if,ilike,immutable,implicit,index,indexes,inherit,inherits,instead,isnull,least,limit,listen,load,location,lock,mode,move,nothing,notify,notnull,nowait,off,offset,oids,operator,owned,owner,password,prepared,procedural,quote,reassign,recheck,reindex,rename,replace,reset,restrict,returning,rule,setof,share,show,stable,statistics,stdin,stdout,storage,strict,sysid,tablespace,temp,template,truncate,trusted,unencrypted,unlisten,until,vacuum,valid,validator,verbose,volatile'"),
            SELECT1_RESULTSET));
  }

  static ResultSet createBitReversedSequenceResultSet(long startValue, long endValue) {
    return ResultSet.newBuilder()
        .setMetadata(
            ResultSetMetadata.newBuilder()
                .setRowType(
                    StructType.newBuilder()
                        .addFields(
                            Field.newBuilder()
                                .setName("n")
                                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                .build())
                        .build())
                .build())
        .addAllRows(
            LongStream.range(startValue, endValue)
                .map(Long::reverse)
                .mapToObj(
                    id ->
                        ListValue.newBuilder()
                            .addValues(
                                Value.newBuilder().setStringValue(String.valueOf(id)).build())
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private static final String FETCH_IDENTIFIERS_SQL =
      "WITH t AS (\n"
          + "\tselect nextval('test_sequence') AS n\n"
          + "\tUNION ALL\n"
          + "\tselect nextval('test_sequence') AS n\n"
          + "\tUNION ALL\n"
          + "\tselect nextval('test_sequence') AS n\n"
          + "\tUNION ALL\n"
          + "\tselect nextval('test_sequence') AS n\n"
          + "\tUNION ALL\n"
          + "\tselect nextval('test_sequence') AS n\n"
          + ")\n"
          + "SELECT n FROM t";

  @Entity
  @Table(name = "test")
  static class TestEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "bit_reversed_sequence")
    @GenericGenerator(
        name = "bit_reversed_sequence",
        type = EnhancedBitReversedSequenceStyleGenerator.class,
        parameters = {
          @Parameter(name = "sequence_name", value = "test_sequence"),
          @Parameter(name = "increment_size", value = "5"),
          @Parameter(name = "initial_value", value = "5000"),
          @Parameter(name = "exclude_ranges", value = "[1,1000] [10000,20000]")
        })
    private Long id;

    @Column private String name;

    protected TestEntity() {}

    public TestEntity(String name) {
      this.name = name;
    }

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  private static final String INSERT_SQL = "insert into test (name,id) values ($1,$2)";

  private static final ResultSet DESCRIBE_INSERT_RESULT_SET =
      ResultSet.newBuilder()
          .setMetadata(
              ResultSetMetadata.newBuilder()
                  .setUndeclaredParameters(
                      StructType.newBuilder()
                          .addFields(
                              Field.newBuilder()
                                  .setName("p1")
                                  .setType(Type.newBuilder().setCode(TypeCode.STRING))
                                  .build())
                          .addFields(
                              Field.newBuilder()
                                  .setName("p2")
                                  .setType(Type.newBuilder().setCode(TypeCode.INT64))
                                  .build())
                          .build())
                  .build())
          .setStats(ResultSetStats.newBuilder().build())
          .build();

  @Test
  public void testGenerateSchema() {
    // Add two responses, as the schema generation results in two separate statements.
    addDdlResponseToSpannerAdmin();
    addDdlResponseToSpannerAdmin();

    //noinspection EmptyTryBlock
    try (SessionFactory ignored =
        createTestHibernateConfig(
                ImmutableList.of(TestEntity.class),
                ImmutableMap.of("hibernate.hbm2ddl.auto", "create-only"))
            .buildSessionFactory()) {}

    List<AbstractMessage> requests = mockDatabaseAdmin.getRequests();
    assertEquals(2, requests.size());

    assertEquals(UpdateDatabaseDdlRequest.class, requests.get(0).getClass());
    UpdateDatabaseDdlRequest createSequenceRequest = (UpdateDatabaseDdlRequest) requests.get(0);
    assertEquals(1, createSequenceRequest.getStatementsCount());
    assertEquals(
        "create sequence test_sequence bit_reversed_positive skip range 1 20000 start counter with 5000",
        createSequenceRequest.getStatements(0));

    assertEquals(UpdateDatabaseDdlRequest.class, requests.get(1).getClass());
    UpdateDatabaseDdlRequest createTableRequest = (UpdateDatabaseDdlRequest) requests.get(1);
    assertEquals(1, createTableRequest.getStatementsCount());
    assertEquals(
        "create table test (id bigint not null, name varchar(255), primary key (id))",
        createTableRequest.getStatements(0));
  }

  @Test
  public void testGenerateIdentifiers() {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(FETCH_IDENTIFIERS_SQL), createBitReversedSequenceResultSet(5000L, 5005L)));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(INSERT_SQL), DESCRIBE_INSERT_RESULT_SET));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(INSERT_SQL)
                .bind("p1")
                .to("test1")
                .bind("p2")
                .to(Long.reverse(5000L))
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(INSERT_SQL)
                .bind("p1")
                .to("test2")
                .bind("p2")
                .to(Long.reverse(5001L))
                .build(),
            1L));

    try (SessionFactory sessionFactory =
            createTestHibernateConfig(
                    ImmutableList.of(TestEntity.class),
                    ImmutableMap.of("hibernate.jdbc.batch_size", "100"))
                .buildSessionFactory();
        Session session = sessionFactory.openSession()) {
      Transaction transaction = session.beginTransaction();
      session.persist(new TestEntity("test1"));
      session.persist(new TestEntity("test2"));
      transaction.commit();
    }

    // There should be three read/write transactions that are committed:
    // 1. A system query executed by Hibernate. Hibernate uses read/write transactions for
    //    everything, even for these system queries.
    // 2. Fetching the identifiers from the sequence should use a separate read/write transaction.
    //    Sequences require read/write transactions, but we do not want the transaction that is used
    //    to fetch the identifiers to be mixed with the main read/write transaction, because if the
    //    main read/write transaction is aborted and retried, it would always fail during retry if
    //    it included fetching identifiers. The latter is because the sequence will return new
    //    values every time, even if the transaction is aborted or rolled back.
    // 3. The main read/write transaction.
    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));

    // There are three ExecuteSql requests:
    // 1. The initial system query.
    // 2. The query to fetch identifiers.
    // 3. A request to describe the insert statement. This is part of the PG wire-protocol.
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest fetchIdentifiersRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(FETCH_IDENTIFIERS_SQL, fetchIdentifiersRequest.getSql());
    assertTrue(fetchIdentifiersRequest.hasTransaction());
    assertTrue(fetchIdentifiersRequest.getTransaction().hasBegin());
    assertTrue(fetchIdentifiersRequest.getTransaction().getBegin().hasReadWrite());

    // the insert statement is described before it is executed.
    ExecuteSqlRequest describeInsertRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2);
    assertEquals(INSERT_SQL, describeInsertRequest.getSql());
    assertEquals(QueryMode.PLAN, describeInsertRequest.getQueryMode());
    assertTrue(describeInsertRequest.hasTransaction());
    assertTrue(describeInsertRequest.getTransaction().hasBegin());
    assertTrue(describeInsertRequest.getTransaction().getBegin().hasReadWrite());
    // The insert statements should be executed as a single batch DML request.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest batchDmlRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, batchDmlRequest.getStatementsCount());
    assertEquals(INSERT_SQL, batchDmlRequest.getStatements(0).getSql());
    assertEquals(INSERT_SQL, batchDmlRequest.getStatements(1).getSql());
    assertTrue(batchDmlRequest.hasTransaction());
    assertTrue(batchDmlRequest.getTransaction().hasId());
  }

  @Test
  public void testGenerateIdentifiersAborted() {
    mockSpanner.putStatementResult(
        StatementResult.queryAndThen(
            Statement.of(FETCH_IDENTIFIERS_SQL),
            createBitReversedSequenceResultSet(5000L, 5005L),
            createBitReversedSequenceResultSet(5005L, 5010L)));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(INSERT_SQL), DESCRIBE_INSERT_RESULT_SET));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(INSERT_SQL)
                .bind("p1")
                .to("test1")
                .bind("p2")
                .to(Long.reverse(5000L))
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(INSERT_SQL)
                .bind("p1")
                .to("test2")
                .bind("p2")
                .to(Long.reverse(5001L))
                .build(),
            1L));

    try (SessionFactory sessionFactory =
            createTestHibernateConfig(
                    ImmutableList.of(TestEntity.class),
                    ImmutableMap.of("hibernate.jdbc.batch_size", "100"))
                .buildSessionFactory();
        Session session = sessionFactory.openSession()) {
      // Make sure the next transaction that commits is aborted.
      // This should be the transaction that fetches the identifier values.
      mockSpanner.setCommitExecutionTime(
          SimulatedExecutionTime.ofException(
              mockSpanner.createAbortedException(ByteString.copyFromUtf8("test"))));
      Transaction transaction = session.beginTransaction();
      session.persist(new TestEntity("test1"));
      session.persist(new TestEntity("test2"));
      transaction.commit();
    }

    // There should be four read/write transactions that are committed:
    // 1. A system query executed by Hibernate. Hibernate uses read/write transactions for
    //    everything, even for these system queries.
    // 2. Fetching the identifiers from the sequence should use a separate read/write transaction.
    //    The first of these transactions is aborted by Cloud Spanner, and then retried.
    // 3. The main read/write transaction.
    assertEquals(4, mockSpanner.countRequestsOfType(CommitRequest.class));

    // There are three ExecuteSql requests:
    // 1. The initial system query.
    // 2. The initial query to fetch identifiers.
    // 3. The retried query to fetch identifiers.
    // 4. A request to describe the insert statement. This is part of the PG wire-protocol.
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    for (int index = 1; index <= 2; index++) {
      ExecuteSqlRequest fetchIdentifiersRequest =
          mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(index);
      assertEquals(FETCH_IDENTIFIERS_SQL, fetchIdentifiersRequest.getSql());
      assertTrue(fetchIdentifiersRequest.hasTransaction());
      assertTrue(fetchIdentifiersRequest.getTransaction().hasBegin());
      assertTrue(fetchIdentifiersRequest.getTransaction().getBegin().hasReadWrite());
    }

    // the insert statement is described before it is executed.
    ExecuteSqlRequest describeInsertRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(3);
    assertEquals(INSERT_SQL, describeInsertRequest.getSql());
    assertEquals(QueryMode.PLAN, describeInsertRequest.getQueryMode());
    assertTrue(describeInsertRequest.hasTransaction());
    assertTrue(describeInsertRequest.getTransaction().hasBegin());
    assertTrue(describeInsertRequest.getTransaction().getBegin().hasReadWrite());
    // The insert statements should be executed as a single batch DML request.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest batchDmlRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, batchDmlRequest.getStatementsCount());
    assertEquals(INSERT_SQL, batchDmlRequest.getStatements(0).getSql());
    assertEquals(INSERT_SQL, batchDmlRequest.getStatements(1).getSql());
    assertTrue(batchDmlRequest.hasTransaction());
    assertTrue(batchDmlRequest.getTransaction().hasId());
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
  }

  protected Configuration createTestHibernateConfig(Iterable<Class<?>> entityClasses) {
    return createTestHibernateConfig(entityClasses, ImmutableMap.of());
  }

  protected Configuration createTestHibernateConfig(
      Iterable<Class<?>> entityClasses, Map<String, String> hibernateProperties) {
    Configuration config = new Configuration();

    config.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    config.setProperty("hibernate.connection.url", createUrl());
    config.setProperty("hibernate.dialect", PostgreSQLDialect.class.getName());
    for (Entry<String, String> property : hibernateProperties.entrySet()) {
      config.setProperty(property.getKey(), property.getValue());
    }
    for (Class<?> entityClass : entityClasses) {
      config.addAnnotatedClass(entityClass);
    }

    return config;
  }
}
