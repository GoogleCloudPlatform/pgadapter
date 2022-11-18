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

package com.google.cloud.spanner.pgadapter.metadata;

import static com.google.cloud.spanner.pgadapter.metadata.DescribeResult.extractParameterTypes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class DescribeResultTest {

  @Test
  public void testExtractParameterTypes() {
    assertArrayEquals(
        new int[] {}, extractParameterTypes(new int[] {}, StructType.newBuilder().build()));
    assertArrayEquals(
        new int[] {Oid.INT8},
        extractParameterTypes(
            new int[] {},
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName("p1")
                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                        .build())
                .build()));
    assertArrayEquals(
        new int[] {Oid.BOOL},
        extractParameterTypes(
            new int[] {},
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName("p1")
                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                        .build())
                .build()));
    assertArrayEquals(
        new int[] {Oid.INT8},
        extractParameterTypes(
            new int[] {Oid.INT8},
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName("p1")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .build()));
    assertArrayEquals(
        new int[] {Oid.INT8},
        extractParameterTypes(new int[] {Oid.INT8}, StructType.newBuilder().build()));
    assertArrayEquals(
        new int[] {Oid.INT8, Oid.VARCHAR},
        extractParameterTypes(
            new int[] {Oid.INT8},
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName("p2")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .build()));
    assertArrayEquals(
        new int[] {Oid.INT8, Oid.UNSPECIFIED, Oid.VARCHAR},
        extractParameterTypes(
            new int[] {Oid.INT8},
            StructType.newBuilder()
                .addFields(
                    Field.newBuilder()
                        .setName("p3")
                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                        .build())
                .build()));

    PGException exception =
        assertThrows(
            PGException.class,
            () ->
                extractParameterTypes(
                    new int[] {},
                    StructType.newBuilder()
                        .addFields(
                            Field.newBuilder()
                                .setName("foo")
                                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                .build())
                        .build()));
    assertEquals("Invalid parameter name: foo", exception.getMessage());
  }

  @Test
  public void testClose() {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getMetadata()).thenReturn(ResultSetMetadata.newBuilder().build());
    DescribeResult result = new DescribeResult(new int[] {}, resultSet);
    result.close();

    verify(resultSet).close();
    result.close();
  }
}
