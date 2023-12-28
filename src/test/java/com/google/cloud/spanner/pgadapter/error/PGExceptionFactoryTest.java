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

import static com.google.cloud.spanner.pgadapter.error.PGExceptionFactory.checkArgument;
import static com.google.cloud.spanner.pgadapter.error.PGExceptionFactory.toPGException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import io.grpc.Status.Code;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PGExceptionFactoryTest {

  @Test
  public void testToPGException() {
    assertEquals(
        PGExceptionFactory.newPGException("Invalid statement"),
        toPGException(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT, "Invalid statement")));
    assertEquals(
        PGExceptionFactory.newPGException("Table not found: foo"),
        toPGException(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION, "Table not found: foo")));
    assertEquals(
        PGExceptionFactory.newPGException("Table not found: foo"),
        toPGException(
            SpannerExceptionFactory.newSpannerException(
                new ApiException(
                    "Table not found: foo",
                    /* cause= */ null,
                    GrpcStatusCode.of(Code.FAILED_PRECONDITION),
                    /* retryable= */ false))));
    assertEquals(
        SQLState.SerializationFailure,
        toPGException(
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.ABORTED, "The transaction was aborted"))
            .getSQLState());
    assertEquals(
        SQLState.UniqueViolation,
        toPGException(
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.ALREADY_EXISTS, "Row [1] in table \"test\" already exists."))
            .getSQLState());
    assertEquals(
        SQLState.UniqueViolation,
        toPGException(
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.ALREADY_EXISTS,
                    "Failed to insert row with primary key [1] due to previously existing row"))
            .getSQLState());
    assertEquals(
        SQLState.UniqueViolation,
        toPGException(
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.ALREADY_EXISTS,
                    "Unique index violation on index my_unique_index at index key [1]"))
            .getSQLState());
    assertEquals(
        SQLState.UniqueViolation,
        toPGException(
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.ALREADY_EXISTS,
                    "UNIQUE violation on index my_unique_index duplicate key: [1]"))
            .getSQLState());
    assertEquals(
        SQLState.ForeignKeyViolation,
        toPGException(
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.FAILED_PRECONDITION,
                    "Foreign key constraint my_foreign_key is violated on table \"test\". Cannot find referenced values in \"referenced_table\""))
            .getSQLState());
    assertEquals(
        SQLState.ForeignKeyViolation,
        toPGException(
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.FAILED_PRECONDITION,
                    "Foreign key my_foreign_key constraint violation on table \"test\". Cannot find referenced key [1] in table \"referenced_table\""))
            .getSQLState());
  }

  @Test
  public void testCheckArgument() {
    assertEquals(1L, checkArgument(1L, true, "test message").longValue());
    PGException exception =
        assertThrows(PGException.class, () -> checkArgument(1L, false, "test message"));
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());
    assertEquals("test message", exception.getMessage());
  }
}
