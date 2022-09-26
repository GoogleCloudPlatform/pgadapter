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

import static com.google.cloud.spanner.pgadapter.error.PGExceptionFactory.toPGException;
import static org.junit.Assert.assertEquals;

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
                ErrorCode.NOT_FOUND, "Table not found: foo")));
    assertEquals(
        PGExceptionFactory.newPGException("Table not found: foo"),
        toPGException(
            SpannerExceptionFactory.newSpannerException(
                new ApiException(
                    "Table not found: foo",
                    /* cause= */ null,
                    GrpcStatusCode.of(Code.NOT_FOUND),
                    /* retryable= */ false))));
  }
}
