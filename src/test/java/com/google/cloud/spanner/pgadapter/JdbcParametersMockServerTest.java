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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.core.Oid;

@RunWith(Parameterized.class)
public class JdbcParametersMockServerTest extends AbstractMockServerTest {
  private static final String JDBC_SQL = "insert into foo values (?)";
  private static final String SPANNER_SQL = "insert into foo values ($1)";

  @Parameter public boolean binaryTransfer;

  @Parameters(name = "binaryTransfer = {0}")
  public static Object[] data() {
    return new Object[] {true, false};
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use binary transfer of
   * for parameters of the given OID.
   */
  private String createUrl(int oid) {
    String url = String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
    if (binaryTransfer) {
      return String.format("%s?binaryTransferEnable=%d", url, oid);
    }
    return url;
  }

  @Test
  public void testBoolParam() throws SQLException {
    // The JDBC driver ignores the setting for binary transfer of BOOL parameters, so this actually
    // only tests the text format.
    testParamTransfer(
        Oid.BOOL,
        new Value[] {
          Value.bool(false), Value.bool(true),
        },
        new Object[] {
          false, true,
        });
  }

  @Test
  public void testInt8Param() throws SQLException {
    testParamTransfer(
        Oid.INT8,
        new Value[] {
          Value.int64(0L),
          Value.int64(1L),
          Value.int64(-1L),
          Value.int64(100L),
          Value.int64(-100L),
          Value.int64(Long.MAX_VALUE),
          Value.int64(Long.MIN_VALUE),
        },
        new Object[] {
          0L, 1L, -1L, 100L, -100L, Long.MAX_VALUE, Long.MIN_VALUE,
        });
  }

  @Test
  public void testInt4Param() throws SQLException {
    testParamTransfer(
        Oid.INT4,
        new Value[] {
          Value.int64(0L),
          Value.int64(1L),
          Value.int64(-1L),
          Value.int64(100L),
          Value.int64(-100L),
          Value.int64(Integer.MAX_VALUE),
          Value.int64(Integer.MIN_VALUE),
        },
        new Object[] {
          0, 1, -1, 100, -100, Integer.MAX_VALUE, Integer.MIN_VALUE,
        });
  }

  @Test
  public void testInt2Param() throws SQLException {
    testParamTransfer(
        Oid.INT2,
        new Value[] {
          Value.int64(0L),
          Value.int64(1L),
          Value.int64(-1L),
          Value.int64(100L),
          Value.int64(-100L),
          Value.int64(Short.MAX_VALUE),
          Value.int64(Short.MIN_VALUE),
        },
        new Object[] {
          (short) 0,
          (short) 1,
          (short) -1,
          (short) 100,
          (short) -100,
          Short.MAX_VALUE,
          Short.MIN_VALUE,
        });
  }

  @Test
  public void testFloat8Param() throws SQLException {
    testParamTransfer(
        Oid.FLOAT8,
        new Value[] {
          Value.float64(0d),
          Value.float64(1d),
          Value.float64(-1d),
          Value.float64(3.14d),
          Value.float64(-3.14d),
          Value.float64(Double.MAX_VALUE),
          Value.float64(Double.MIN_VALUE),
          Value.float64(Double.MIN_NORMAL),
        },
        new Object[] {
          0d, 1d, -1d, 3.14d, -3.14d, Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_NORMAL,
        });
  }

  @Test
  public void testFloat4Param() throws SQLException {
    testParamTransfer(
        Oid.FLOAT4,
        new Value[] {
          Value.float32(0f),
          Value.float32(1f),
          Value.float32(-1f),
          Value.float32(3.14f),
          Value.float32(-3.14f),
          Value.float32(Float.MAX_VALUE),
          Value.float32(Float.MIN_VALUE),
          Value.float32(Float.MIN_NORMAL),
        },
        new Object[] {
          0f, 1f, -1f, 3.14f, -3.14f, Float.MAX_VALUE, Float.MIN_VALUE, Float.MIN_NORMAL,
        });
  }

  @Test
  public void testNumericParam() throws SQLException {
    testParamTransfer(
        Oid.NUMERIC,
        new Value[] {
          Value.pgNumeric("0"),
          Value.pgNumeric("1"),
          Value.pgNumeric("-1"),
          Value.pgNumeric("3.14"),
          Value.pgNumeric("-3.14"),
          // Value.pgNumeric("NaN"),
        },
        new Object[] {
          BigDecimal.ZERO,
          new BigDecimal(1),
          new BigDecimal(-1),
          new BigDecimal("3.14"),
          new BigDecimal("-3.14"),
          // The PG JDBC driver does not support NaN NUMERIC parameters.
          // Double.NaN,
        });
  }

  @Test
  public void testTextParam() throws SQLException {
    for (int oid : new int[] {Oid.TEXT, Oid.VARCHAR}) {
      testParamTransfer(
          oid,
          new Value[] {
            Value.string("test"), Value.string(""), Value.string("ㅁㄲ"),
          },
          new Object[] {
            "test", "", "ㅁㄲ",
          });
    }
  }

  @Test
  public void testJsonbParam() throws SQLException {
    for (int oid : new int[] {Oid.JSONB}) {
      testParamTransfer(
          oid,
          new Value[] {
            Value.string("{\"key\":\"value\"}"), Value.string("{}"), Value.string("[]"),
          },
          new Object[] {
            "{\"key\":\"value\"}", "{}", "[]",
          });
    }
  }

  @Test
  public void testDateParam() throws SQLException {
    testParamTransfer(
        Oid.DATE,
        new Value[] {
          Value.date(Date.parseDate("2022-04-19")),
          Value.date(Date.parseDate("0001-01-01")),
          Value.date(Date.parseDate("9999-12-31")),
          Value.date(Date.parseDate("2000-02-29")),
          Value.date(Date.parseDate("1970-01-01")),
          Value.date(Date.parseDate("2000-01-01")),
        },
        new Object[] {
          LocalDate.parse("2022-04-19"),
          LocalDate.parse("0001-01-01"),
          LocalDate.parse("9999-12-31"),
          LocalDate.parse("2000-02-29"),
          LocalDate.parse("1970-01-01"),
          LocalDate.parse("2000-01-01"),
        });
  }

  @Test
  public void testTimestampParam() throws SQLException {
    testParamTransfer(
        Oid.TIMESTAMPTZ,
        new Value[] {
          Value.timestamp(Timestamp.parseTimestamp("2022-04-19T16:06:23.424932+02:00")),
          Value.timestamp(Timestamp.parseTimestamp("0001-01-01T00:00:00Z")),
          Value.timestamp(Timestamp.parseTimestamp("9999-12-31T23:59:59.999999Z")),
          Value.timestamp(Timestamp.parseTimestamp("2000-02-29T00:00:00Z")),
          Value.timestamp(Timestamp.parseTimestamp("2000-03-01T03:10:01.123457000Z")),
          Value.timestamp(Timestamp.parseTimestamp("1970-01-01T00:00:00Z")),
          Value.timestamp(Timestamp.parseTimestamp("2000-01-01T00:00:00Z")),
        },
        new Object[] {
          OffsetDateTime.parse("2022-04-19T16:06:23.424932+02:00"),
          OffsetDateTime.parse("0001-01-01T00:00:00Z"),
          OffsetDateTime.parse("9999-12-31T23:59:59.999999Z"),
          OffsetDateTime.parse("2000-02-29T00:00:00Z"),
          OffsetDateTime.parse("2000-03-01T03:10:01.123456789Z"),
          OffsetDateTime.parse("1970-01-01T00:00:00Z"),
          OffsetDateTime.parse("2000-01-01T00:00:00Z"),
        });
  }

  @Test
  public void testByteaParam() throws SQLException {
    testParamTransfer(
        Oid.BYTEA,
        new Value[] {
          Value.bytes(ByteArray.copyFrom("test")),
          Value.bytes(ByteArray.copyFrom("")),
          Value.bytes(ByteArray.copyFrom("ㅁㄲ")),
        },
        new Object[] {
          "test".getBytes(StandardCharsets.UTF_8),
          "".getBytes(StandardCharsets.UTF_8),
          "ㅁㄲ".getBytes(StandardCharsets.UTF_8),
        });
  }

  private void testParamTransfer(int oid, Value[] spannerValues, Object[] jdbcValues)
      throws SQLException {
    for (int index = 0; index < spannerValues.length; index++) {
      mockSpanner.putStatementResult(
          StatementResult.update(
              Statement.newBuilder(SPANNER_SQL).bind("p1").to(spannerValues[index]).build(),
              index));

      String url = createUrl(oid);
      try (Connection connection = DriverManager.getConnection(url)) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(JDBC_SQL)) {
          preparedStatement.setObject(1, jdbcValues[index]);
          assertEquals(index, preparedStatement.executeLargeUpdate());
        }
      }
    }
  }
}
