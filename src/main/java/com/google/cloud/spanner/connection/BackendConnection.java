// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.connection;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceNotFoundException;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.myadapter.error.MyException;
import com.google.cloud.spanner.myadapter.error.SQLState;
import com.google.cloud.spanner.myadapter.error.Severity;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.statements.SessionStatementParser.SessionStatement;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class BackendConnection {
  private static final Logger logger = Logger.getLogger(BackendConnection.class.getName());

  private static final String CHANNEL_PROVIDER_PROPERTY = "CHANNEL_PROVIDER";

  private final OptionsMetadata options;
  private final Properties serverProperties;
  private Connection spannerConnection;
  private DatabaseId databaseId;

  public BackendConnection(
      OptionsMetadata options, Properties serverProperties, Connection spannerConnection) {
    this.options = options;
    this.serverProperties = serverProperties;
    this.spannerConnection = spannerConnection;
  }

  @InternalApi
  public void connectToSpanner(String database, @Nullable Credentials credentials) {
    String uri =
        options.hasDefaultConnectionUrl()
            ? options.getDefaultConnectionUrl()
            : options.buildConnectionURL(database);
    if (uri.startsWith("jdbc:")) {
      uri = uri.substring("jdbc:".length());
    }
    uri = appendPropertiesToUrl(uri, serverProperties);
    if (System.getProperty(CHANNEL_PROVIDER_PROPERTY) != null) {
      uri =
          uri
              + ";"
              + ConnectionOptions.CHANNEL_PROVIDER_PROPERTY_NAME
              + "="
              + System.getProperty(CHANNEL_PROVIDER_PROPERTY);
      // This forces the connection to use NoCredentials.
      uri = uri + ";usePlainText=true";
      try {
        Class.forName(System.getProperty(CHANNEL_PROVIDER_PROPERTY));
      } catch (ClassNotFoundException e) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown or invalid channel provider: "
                + System.getProperty(CHANNEL_PROVIDER_PROPERTY));
      }
    }
    ConnectionOptions.Builder connectionOptionsBuilder = ConnectionOptions.newBuilder().setUri(uri);
    if (credentials != null) {
      connectionOptionsBuilder =
          ConnectionOptionsHelper.setCredentials(connectionOptionsBuilder, credentials);
    }
    ConnectionOptions connectionOptions = connectionOptionsBuilder.build();
    logger.log(Level.INFO, "Connecting to spanner!");
    Connection spannerConnection = connectionOptions.getConnection();
    logger.log(Level.INFO, "Spanner connection establised!");
    try {
      // Note: Calling getDialect() will cause a SpannerException if the connection itself is
      // invalid, for example as a result of the credentials being wrong.
      if (spannerConnection.getDialect() != Dialect.GOOGLE_STANDARD_SQL) {
        spannerConnection.close();
        throw MyException.newBuilder(
                String.format(
                    "The database uses dialect %s. Currently PGAdapter only supports connections to PostgreSQL dialect databases. "
                        + "These can be created using https://cloud.google.com/spanner/docs/quickstart-console#postgresql",
                    spannerConnection.getDialect()))
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.SQLServerRejectedEstablishmentOfSQLConnection)
            .build();
      }
    } catch (InstanceNotFoundException | DatabaseNotFoundException notFoundException) {
      SpannerException exceptionToThrow = notFoundException;
      spannerConnection.close();
      throw exceptionToThrow;
    } catch (SpannerException e) {
      spannerConnection.close();
      throw e;
    }
    this.spannerConnection = spannerConnection;
    this.databaseId = connectionOptions.getDatabaseId();
  }

  public StatementResult executeQuery(
      Statement statement, ParsedStatement parsedStatement, SessionState sessionState) {
    return spannerConnection.execute(statement);
  }

  public StatementResult executeSessionStatement(
      SessionStatement sessionStatement, SessionState sessionState) {
    StatementResult statementResult = sessionStatement.execute(sessionState, this);
    return statementResult;
  }

  public void terminate() {
    if (this.spannerConnection != null) {
      this.spannerConnection.close();
    }
  }

  public boolean isTransactionActive() {
    return spannerConnection.isInTransaction();
  }

  public void commit() {
    spannerConnection.commit();
  }

  public void setAutocommit(boolean autocommit) {
    spannerConnection.setAutocommit(autocommit);
  }

  private String appendPropertiesToUrl(String url, Properties info) {
    if (info == null || info.isEmpty()) {
      return url;
    }
    StringBuilder result = new StringBuilder(url);
    for (Entry<Object, Object> entry : info.entrySet()) {
      if (entry.getValue() != null && !"".equals(entry.getValue())) {
        result.append(";").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    return result.toString();
  }

  public void processSetAutocommit() {
    if (this.isTransactionActive()) {
      this.commit();
    }
    this.setAutocommit(true);
  }

  public void processUnsetAutocommit() {
    this.setAutocommit(false);
  }
}
