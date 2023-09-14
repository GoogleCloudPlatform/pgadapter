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

import static org.hibernate.id.enhanced.SequenceStyleGenerator.SEQUENCE_PARAM;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.boot.model.relational.QualifiedSequenceName;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.NoopOptimizer;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;
import org.postgresql.util.PSQLException;

/**
 * Sequence generator that uses a bit-reversed sequence and that fetches multiple values from the
 * sequence in a single round-trip. This ensures that the generator is compatible with batching.
 */
public class EnhancedBitReversedSequenceStyleGenerator
    implements BulkInsertionCapableIdentifierGenerator, PersistentIdentifierGenerator {

  /**
   * The default increment (fetch) size for an {@link EnhancedBitReversedSequenceStyleGenerator}.
   */
  public static final int DEFAULT_INCREMENT_SIZE = 100;

  /**
   * Configuration property for defining ranges that should be excluded by a bit-reversed sequence
   * generator.
   */
  public static final String EXCLUDE_RANGES_PARAM = "exclude_ranges";

  private static final Iterator<Long> EMPTY_ITERATOR = ImmutableList.<Long>of().iterator();
  private final Lock lock = new ReentrantLock();

  private QualifiedSequenceName sequenceName;
  private String select;
  private int fetchSize;
  private Iterator<Long> identifiers = EMPTY_ITERATOR;
  private DatabaseStructure databaseStructure;
  private final Optimizer optimizer = new NoopOptimizer(Long.class, 1);

  private static String buildSelect(
      Dialect dialect, QualifiedSequenceName sequenceName, int fetchSize) {
    return "WITH t AS (\n"
        + IntStream.range(0, fetchSize)
        .mapToObj(
            ignore ->
                "\t"
                    + dialect
                    .getSequenceSupport()
                    .getSequenceNextValString(sequenceName.render())
                    + " AS n")
        .collect(Collectors.joining("\n\tUNION ALL\n"))
        + "\n)\n"
        + "SELECT n FROM t";
  }

  private static BitReversedSequenceStructure buildDatabaseStructure(
      String contributor,
      Type type,
      QualifiedSequenceName sequenceName,
      int initialValue,
      List<Range<Long>> excludeRanges,
      JdbcEnvironment jdbcEnvironment) {
    return new BitReversedSequenceStructure(
        jdbcEnvironment,
        contributor,
        sequenceName,
        initialValue,
        1,
        excludeRanges,
        type.getReturnedClass());
  }

  private static QualifiedSequenceName determineSequenceName(
      JdbcEnvironment jdbcEnvironment, Properties params) {
    String sequenceName = params.getProperty(SEQUENCE_PARAM);
    if (sequenceName == null) {
      throw new MappingException("no sequence name specified");
    }
    if (sequenceName.contains(".")) {
      QualifiedName qualifiedName = QualifiedNameParser.INSTANCE.parse(sequenceName);
      return new QualifiedSequenceName(
          qualifiedName.getCatalogName(),
          qualifiedName.getSchemaName(),
          qualifiedName.getObjectName());
    } else {
      final Identifier catalog =
          jdbcEnvironment
              .getIdentifierHelper()
              .toIdentifier(ConfigurationHelper.getString(CATALOG, params));
      final Identifier schema =
          jdbcEnvironment
              .getIdentifierHelper()
              .toIdentifier(ConfigurationHelper.getString(SCHEMA, params));
      return new QualifiedSequenceName(
          catalog, schema, jdbcEnvironment.getIdentifierHelper().toIdentifier(sequenceName));
    }
  }

  private static int determineFetchSize(Properties params) {
    int fetchSize;
    if (ConfigurationHelper.getInteger("fetch_size", params) != null) {
      fetchSize = ConfigurationHelper.getInt("fetch_size", params, DEFAULT_INCREMENT_SIZE);
    } else {
      fetchSize =
          ConfigurationHelper.getInt(
              SequenceStyleGenerator.INCREMENT_PARAM, params, DEFAULT_INCREMENT_SIZE);
    }
    if (fetchSize <= 0) {
      throw new MappingException("increment size must be positive");
    }
    return fetchSize;
  }

  private static int determineInitialValue(Properties params) {
    int initialValue =
        ConfigurationHelper.getInt(
            SequenceStyleGenerator.INITIAL_PARAM,
            params,
            SequenceStyleGenerator.DEFAULT_INITIAL_VALUE);
    if (initialValue <= 0) {
      throw new MappingException("initial value must be positive");
    }
    return initialValue;
  }

  @Override
  public Optimizer getOptimizer() {
    return optimizer;
  }

  @Override
  public void configure(Type type, Properties params, ServiceRegistry serviceRegistry)
      throws MappingException {
    JdbcEnvironment jdbcEnvironment = serviceRegistry.getService(JdbcEnvironment.class);
    this.sequenceName = determineSequenceName(jdbcEnvironment, params);
    this.fetchSize = determineFetchSize(params);
    int initialValue = determineInitialValue(params);
    this.select = buildSelect(jdbcEnvironment.getDialect(), sequenceName, fetchSize);
    List<Range<Long>> excludeRanges =
        parseExcludedRanges(sequenceName.getObjectName().getText(), params);
    String contributor = determineContributor(params);
    this.databaseStructure =
        buildDatabaseStructure(
            contributor, type, sequenceName, initialValue, excludeRanges, jdbcEnvironment);
  }

  private String determineContributor(Properties params) {
    final String contributor = params.getProperty(IdentifierGenerator.CONTRIBUTOR_NAME);
    return contributor == null ? "orm" : contributor;
  }

  @Override
  public String determineBulkInsertionIdentifierGenerationSelectFragment(
      SqlStringGenerationContext context) {
    return context
        .getDialect()
        .getSequenceSupport()
        .getSelectSequenceNextValString(getSequenceName());
  }

  @Override
  public Serializable generate(SharedSessionContractImplementor session, Object object)
      throws HibernateException {
    this.lock.lock();
    try {
      while (!this.identifiers.hasNext()) {
        this.identifiers = fetchIdentifiers(session);
      }
      return this.identifiers.next();
    } finally {
      this.lock.unlock();
    }
  }

  private String getSequenceName() {
    return this.databaseStructure.getPhysicalName().getObjectName().getCanonicalName();
  }

  @Override
  public void registerExportables(Database database) {
    Namespace namespace =
        database.locateNamespace(sequenceName.getCatalogName(), sequenceName.getSchemaName());
    Sequence sequence = namespace.locateSequence(sequenceName.getSequenceName());
    if (sequence == null) {
      this.databaseStructure.registerExportables(database);
    }
  }

  private Iterator<Long> fetchIdentifiers(SharedSessionContractImplementor session)
      throws HibernateException {
    Connection connection = null;
    Boolean retryAbortsInternally = null;
    try {
      // Use a separate connection to get new sequence values. This ensures that it also uses a
      // separate read/write transaction, which again means that it will not interfere with any
      // retries of the actual business transaction.
      connection = session.getJdbcConnectionAccess().obtainConnection();
      try (Statement statement = connection.createStatement()) {
        // TODO: Use 'set local spanner.retry_aborts_internally=false' when that has been
        //       implemented.
        retryAbortsInternally = isRetryAbortsInternally(statement);
        statement.execute("set spanner.retry_aborts_internally=false");
        statement.execute("begin transaction");
        statement.execute("set transaction read write");
        List<Long> identifiers = new ArrayList<>(this.fetchSize);
        try (ResultSet resultSet = statement.executeQuery(this.select)) {
          while (resultSet.next()) {
            identifiers.add(resultSet.getLong(1));
          }
        }
        statement.execute("commit");
        return identifiers.iterator();
      }
    } catch (SQLException sqlException) {
      if (connection != null) {
        Connection finalConnection = connection;
        ignoreSqlException(() -> finalConnection.createStatement().execute("rollback"));
      }
      if (sqlException instanceof PSQLException) {
        PSQLException psqlException = (PSQLException) sqlException;
        // Check if the error is caused by an aborted transaction.
        if (psqlException.getServerErrorMessage() != null
            && Objects.equals(
            /* '40001' == serialization_failure */ "40001",
            psqlException.getServerErrorMessage().getSQLState())) {
          // Return an empty iterator to force a retry.
          return EMPTY_ITERATOR;
        }
      }
      throw session
          .getJdbcServices()
          .getSqlExceptionHelper()
          .convert(sqlException, "could not get next sequence values", this.select);
    } finally {
      if (connection != null) {
        Connection finalConnection = connection;
        if (retryAbortsInternally != null) {
          Boolean finalRetryAbortsInternally = retryAbortsInternally;
          ignoreSqlException(
              () ->
                  finalConnection
                      .createStatement()
                      .execute(
                          "set spanner.retry_aborts_internally=" + finalRetryAbortsInternally));
        }
        ignoreSqlException(
            () -> session.getJdbcConnectionAccess().releaseConnection(finalConnection));
      }
    }
  }

  private Boolean isRetryAbortsInternally(Statement statement) {
    try (ResultSet resultSet = statement.executeQuery("show spanner.retry_aborts_internally")) {
      if (resultSet.next()) {
        return resultSet.getBoolean(1);
      }
      return null;
    } catch (Throwable ignore) {
      return null;
    }
  }

  private interface SqlRunnable {
    void run() throws SQLException;
  }

  private void ignoreSqlException(SqlRunnable runnable) {
    try {
      runnable.run();
    } catch (SQLException ignore) {
      // ignore any SQLException
    }
  }

  @Override
  public String toString() {
    return getSequenceName();
  }

  @VisibleForTesting
  static List<Range<Long>> parseExcludedRanges(String sequenceName, Properties params) {
    String[] excludedRangesArray =
        ConfigurationHelper.toStringArray(EXCLUDE_RANGES_PARAM, " ", params);
    if (excludedRangesArray == null) {
      return ImmutableList.of();
    }
    Builder<Range<Long>> builder = ImmutableList.builder();
    for (String rangeString : excludedRangesArray) {
      rangeString = rangeString.trim();
      String invalidRangeMessage =
          String.format(
              "Invalid range found for the [%s] sequence: %%s\n"
                  + "Excluded ranges must be given as a space-separated sequence of ranges between "
                  + "square brackets, e.g. '[1,1000] [2001,3000]'. "
                  + "Found '%s'",
              sequenceName, rangeString);
      if (!(rangeString.startsWith("[") && rangeString.endsWith("]"))) {
        throw new MappingException(
            String.format(invalidRangeMessage, "Range is not enclosed between '[' and ']'"));
      }
      rangeString = rangeString.substring(1, rangeString.length() - 1);
      String[] values = rangeString.split(",");
      if (values.length != 2) {
        throw new MappingException(
            String.format(invalidRangeMessage, "Range does not contain exactly two elements"));
      }
      long from;
      long to;
      try {
        from = Long.parseLong(values[0]);
        to = Long.parseLong(values[1]);
        builder.add(Range.closed(from, to));
      } catch (IllegalArgumentException e) {
        throw new MappingException(String.format(invalidRangeMessage, e.getMessage()), e);
      }
    }
    return builder.build();
  }
}
