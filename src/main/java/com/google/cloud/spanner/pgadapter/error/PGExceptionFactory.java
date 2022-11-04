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
import com.google.cloud.spanner.SpannerException;
import io.grpc.StatusRuntimeException;

/** Factory class for {@link PGException} instances. */
@InternalApi
public class PGExceptionFactory {
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
      String fullGrpcPrefix =
          spannerException.getErrorCode().name()
              + ": "
              + StatusRuntimeException.class.getName()
              + ": "
              + spannerException.getErrorCode().name()
              + ": ";
      String grpcPrefix =
          StatusRuntimeException.class.getName() + ": " + spannerException.getErrorCode().name();
      if (spannerException.getMessage().startsWith(fullGrpcPrefix)) {
        result = spannerException.getMessage().substring(fullGrpcPrefix.length());
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
