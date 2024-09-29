// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.ProxyServer.ShutdownMode;
import com.google.cloud.spanner.pgadapter.ShutdownHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.ShutdownStatement.ParsedShutdownStatement.Builder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;

/**
 * <code>SHUTDOWN [FAST | SMART | IMMEDIATE]</code>
 *
 * <p>Shuts down the proxy server. This statement is a PGAdapter-only statement, and is not
 * supported on normal PostgreSQL.
 */
public class ShutdownStatement extends IntermediatePortalStatement {
  static final class ParsedShutdownStatement {
    static final class Builder {
      ShutdownMode shutdownMode = ShutdownMode.FAST;
    }

    /** The server that will be shut down. */
    final ProxyServer server;

    final ShutdownMode shutdownMode;

    private ParsedShutdownStatement(Builder builder, ProxyServer server) {
      this.server = server;
      this.shutdownMode = builder.shutdownMode;
    }
  }

  private final ParsedShutdownStatement parsedShutdownStatement;

  private ShutdownHandler shutdownHandler;

  public ShutdownStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(
        name,
        new IntermediatePreparedStatement(
            connectionHandler,
            options,
            name,
            NO_PARAMETER_TYPES,
            parsedStatement,
            originalStatement),
        NO_PARAMS,
        ImmutableList.of(),
        ImmutableList.of());
    this.parsedShutdownStatement = parse(originalStatement.getSql(), connectionHandler.getServer());
  }

  @Override
  public String getCommandTag() {
    return "SHUTDOWN";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.UNKNOWN;
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (this.executed && this.shutdownHandler != null) {
      this.shutdownHandler.shutdown(this.parsedShutdownStatement.shutdownMode);
    }
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    if (!this.executed) {
      this.executed = true;
      if (options.isAllowShutdownStatement()) {
        this.shutdownHandler = ShutdownHandler.createForServer(this.parsedShutdownStatement.server);
        setFutureStatementResult(Futures.immediateFuture(new NoResult(getCommandTag())));
      } else {
        setFutureStatementResult(
            Futures.immediateFailedFuture(
                PGExceptionFactory.newPGException(
                    "SHUTDOWN [SMART | FAST | IMMEDIATE] statement is not enabled for this server. Start PGAdapter with --allow_shutdown_statement to enable the use of the SHUTDOWN statement.",
                    SQLState.FeatureNotSupported)));
      }
    }
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this SHUTDOWN statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // SHUTDOWN does not support binding any parameters, so we just return the same statement.
    return this;
  }

  static ParsedShutdownStatement parse(String sql, ProxyServer server) {
    Preconditions.checkNotNull(sql);
    Preconditions.checkNotNull(server);

    SimpleParser parser = new SimpleParser(sql);
    ParsedShutdownStatement.Builder builder = new Builder();
    if (!parser.eatKeyword("shutdown")) {
      throw PGExceptionFactory.newPGException(
          "not a valid SHUTDOWN statement: " + sql, SQLState.SyntaxError);
    }
    if (parser.eatKeyword("fast")) {
      builder.shutdownMode = ShutdownMode.FAST;
    } else if (parser.eatKeyword("smart")) {
      builder.shutdownMode = ShutdownMode.SMART;
    } else if (parser.eatKeyword("immediate")) {
      builder.shutdownMode = ShutdownMode.IMMEDIATE;
    }
    parser.throwIfHasMoreTokens();
    return new ParsedShutdownStatement(builder, server);
  }
}
