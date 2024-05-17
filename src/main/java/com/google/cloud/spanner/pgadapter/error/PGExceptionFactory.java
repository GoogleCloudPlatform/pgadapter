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

package com.google.cloud.spanner.pgadapter.error;

import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.TRANSACTION_ABORTED_ERROR;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import io.grpc.StatusRuntimeException;
import java.util.regex.Pattern;

/** Factory class for {@link PGException} instances. */
@InternalApi
public class PGExceptionFactory {
  private static final Pattern RELATION_NOT_FOUND_PATTERN =
      Pattern.compile("relation .+ does not exist");
  private static final Pattern COLUMN_NOT_FOUND_PATTERN =
      Pattern.compile("column .+ of relation .+ does not exist");
  private static final Pattern CANNOT_DROP_TABLE_WITH_INDICES_PATTERN =
      Pattern.compile("Cannot drop table .+ with indices");
  private static final Pattern ONLY_RESTRICT_BEHAVIOR =
      Pattern.compile("Only <RESTRICT> behavior is supported by <DROP> statement\\.");
  private static final Pattern PK_VIOLATION_PATTERN =
      Pattern.compile("Row .+ in table .+ already exists");
  private static final Pattern PK_VIOLATION_PATTERN_EMULATOR =
      Pattern.compile("Failed to insert row with primary key .+ due to previously existing row");
  private static final Pattern UNIQUE_INDEX_VIOLATION_PATTERN =
      Pattern.compile("Unique index violation on index .+ at index key .+");
  private static final Pattern UNIQUE_INDEX_VIOLATION_PATTERN_EMULATOR =
      Pattern.compile("UNIQUE violation on index .+ duplicate key: .+");
  private static final Pattern FOREIGN_KEY_VIOLATION_PATTERN =
      Pattern.compile(
          "Foreign key constraint .+ is violated on table .+\\. Cannot find referenced values in .+");
  private static final Pattern FOREIGN_KEY_VIOLATION_PATTERN_EMULATOR =
      Pattern.compile(
          "Foreign key .+ constraint violation on table .+\\. Cannot find referenced key .+ in table .+");

  private PGExceptionFactory() {}

  /**
   * Throws a {@link PGException} with the given message and {@link SQLState#InvalidParameterValue}
   * if valid is false. Otherwise, returns the given value.
   */
  public static <T> T checkArgument(T value, boolean valid, String message) {
    if (!valid) {
      throw newPGException(message, SQLState.InvalidParameterValue);
    }
    return value;
  }

  /**
   * Creates a basic {@link PGException} with {@link Severity#ERROR} and {@link
   * SQLState#RaiseException}.
   */
  public static PGException newPGException(String message) {
    return newPGException(message, SQLState.RaiseException);
  }

  /**
   * Creates a basic {@link PGException} with {@link Severity#ERROR} and the specified {@link
   * SQLState}.
   */
  public static PGException newPGException(String message, SQLState sqlState) {
    return PGException.newBuilder(message)
        .setSeverity(Severity.ERROR)
        .setSQLState(sqlState)
        .build();
  }

  /** Creates a new exception that indicates that the current query was cancelled by the client. */
  public static PGException newQueryCancelledException() {
    return newPGException("Query cancelled", SQLState.QueryCanceled);
  }

  /**
   * Creates a new exception that indicates that the current transaction is in the aborted state.
   */
  public static PGException newTransactionAbortedException() {
    return newPGException(TRANSACTION_ABORTED_ERROR, SQLState.InFailedSqlTransaction);
  }

  /** Converts the given {@link SpannerException} to a {@link PGException}. */
  public static PGException toPGException(SpannerException spannerException) {
    if (spannerException.getErrorCode() == ErrorCode.ABORTED) {
      return PGException.newBuilder(extractMessage(spannerException))
          // This is the equivalent of an Aborted transaction error in Cloud Spanner.
          // A client should be prepared to retry this.
          .setSQLState(SQLState.SerializationFailure)
          .build();
    }
    if ((spannerException.getErrorCode() == ErrorCode.NOT_FOUND
            || spannerException.getErrorCode() == ErrorCode.INVALID_ARGUMENT)
        && COLUMN_NOT_FOUND_PATTERN.matcher(spannerException.getMessage()).find()) {
      return PGException.newBuilder(extractMessage(spannerException))
          .setSQLState(SQLState.UndefinedColumn)
          .build();
    } else if ((spannerException.getErrorCode() == ErrorCode.NOT_FOUND
            || spannerException.getErrorCode() == ErrorCode.INVALID_ARGUMENT)
        && RELATION_NOT_FOUND_PATTERN.matcher(spannerException.getMessage()).find()) {
      return PGException.newBuilder(extractMessage(spannerException))
          .setSQLState(SQLState.UndefinedTable)
          .build();
    } else if (spannerException.getErrorCode() == ErrorCode.ALREADY_EXISTS
        && (PK_VIOLATION_PATTERN.matcher(spannerException.getMessage()).find()
            || PK_VIOLATION_PATTERN_EMULATOR.matcher(spannerException.getMessage()).find()
            || UNIQUE_INDEX_VIOLATION_PATTERN.matcher(spannerException.getMessage()).find()
            || UNIQUE_INDEX_VIOLATION_PATTERN_EMULATOR
                .matcher(spannerException.getMessage())
                .find())) {
      return PGException.newBuilder(extractMessage(spannerException))
          .setSQLState(SQLState.UniqueViolation)
          .build();
    } else if (spannerException.getErrorCode() == ErrorCode.FAILED_PRECONDITION
        && (FOREIGN_KEY_VIOLATION_PATTERN_EMULATOR.matcher(spannerException.getMessage()).find()
            || FOREIGN_KEY_VIOLATION_PATTERN.matcher(spannerException.getMessage()).find())) {
      return PGException.newBuilder(extractMessage(spannerException))
          .setSQLState(SQLState.ForeignKeyViolation)
          .build();
    } else if ((spannerException.getErrorCode() == ErrorCode.FAILED_PRECONDITION
            || spannerException.getErrorCode() == ErrorCode.INVALID_ARGUMENT)
        && CANNOT_DROP_TABLE_WITH_INDICES_PATTERN.matcher(spannerException.getMessage()).find()) {
      return PGException.newBuilder(extractMessage(spannerException))
          .setSQLState(SQLState.FeatureNotSupported)
          .setHints(
              "Execute 'set spanner.support_drop_cascade=true' to enable dropping tables with indices")
          .build();
    } else if ((spannerException.getErrorCode() == ErrorCode.FAILED_PRECONDITION
            || spannerException.getErrorCode() == ErrorCode.INVALID_ARGUMENT)
        && ONLY_RESTRICT_BEHAVIOR.matcher(spannerException.getMessage()).find()) {
      return PGException.newBuilder(extractMessage(spannerException))
          .setSQLState(SQLState.FeatureNotSupported)
          .setHints(
              "Execute 'set spanner.support_drop_cascade=true' to enable 'drop {table|schema} cascade' statements.")
          .build();
    }
    return newPGException(extractMessage(spannerException));
  }

  /** Converts the given {@link Exception} to a {@link PGException}. */
  public static PGException toPGException(Throwable throwable) {
    if (throwable instanceof SpannerException) {
      return toPGException((SpannerException) throwable);
    }
    if (throwable instanceof PGException) {
      return (PGException) throwable;
    }
    return newPGException(
        throwable.getMessage() == null ? throwable.getClass().getName() : throwable.getMessage());
  }

  private static final String NOT_FOUND_PREFIX =
      "NOT_FOUND: com.google.api.gax.rpc.NotFoundException: io.grpc.StatusRuntimeException: NOT_FOUND: ";
  private static final String INVALID_ARGUMENT_PREFIX =
      "INVALID_ARGUMENT: com.google.api.gax.rpc.InvalidArgumentException: io.grpc.StatusRuntimeException: INVALID_ARGUMENT: ";

  /** Extracts the base error message from a {@link SpannerException}. */
  static String extractMessage(SpannerException spannerException) {
    String result;
    if (spannerException.getMessage().startsWith(NOT_FOUND_PREFIX)) {
      result = spannerException.getMessage().substring(NOT_FOUND_PREFIX.length());
    } else if (spannerException.getMessage().startsWith(INVALID_ARGUMENT_PREFIX)) {
      result = spannerException.getMessage().substring(INVALID_ARGUMENT_PREFIX.length());
    } else {
      String grpcPrefix =
          spannerException.getErrorCode().name()
              + ": "
              + StatusRuntimeException.class.getName()
              + ": "
              + spannerException.getErrorCode().name()
              + ": ";
      if (spannerException.getMessage().startsWith(grpcPrefix)) {
        result = spannerException.getMessage().substring(grpcPrefix.length());
      } else {
        String spannerPrefix = spannerException.getErrorCode().name() + ": ";
        result =
            spannerException.getMessage().startsWith(spannerPrefix)
                ? spannerException.getMessage().substring(spannerPrefix.length())
                : spannerException.getMessage();
      }
    }
    if (result.startsWith("[ERROR] ")) {
      result = result.substring("[ERROR] ".length());
    }
    return result;
  }
}
