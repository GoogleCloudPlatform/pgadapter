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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.SpannerException;
import io.grpc.StatusRuntimeException;

/** Factory class for {@link PGException} instances. */
@InternalApi
public class PGExceptionFactory {
  private PGExceptionFactory() {}

  /**
   * Creates a basic {@link PGException} with {@link Severity#ERROR} and {@link
   * SQLState#RaiseException}.
   */
  public static PGException newPGException(String message) {
    return PGException.newBuilder(message)
        .setSeverity(Severity.ERROR)
        .setSQLState(SQLState.RaiseException)
        .build();
  }

  /** Converts the given {@link SpannerException} to a {@link PGException}. */
  public static PGException toPGException(SpannerException spannerException) {
    return newPGException(extractMessage(spannerException));
  }

  /** Converts the given {@link Exception} to a {@link PGException}. */
  public static PGException toPGException(Exception exception) {
    if (exception instanceof SpannerException) {
      return toPGException((SpannerException) exception);
    }
    return newPGException(exception.getMessage());
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
