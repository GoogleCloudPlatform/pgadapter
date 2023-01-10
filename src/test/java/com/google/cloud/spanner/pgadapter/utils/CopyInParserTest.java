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

package com.google.cloud.spanner.pgadapter.utils;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.Format;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CopyInParserTest {

  @Test
  public void testCreateText() throws IOException {
    CopyInParser parser =
        CopyInParser.create(
            mock(SessionState.class),
            Format.TEXT,
            CSVFormat.POSTGRESQL_TEXT,
            new PipedInputStream(new PipedOutputStream(), 256),
            false);
    assertTrue(parser instanceof CsvCopyParser);
  }

  @Test
  public void testCreateCsv() throws IOException {
    CopyInParser parser =
        CopyInParser.create(
            mock(SessionState.class),
            Format.CSV,
            CSVFormat.POSTGRESQL_CSV,
            new PipedInputStream(new PipedOutputStream(), 256),
            false);
    assertTrue(parser instanceof CsvCopyParser);
  }

  @Test
  public void testCreateBinary() throws IOException {
    CopyInParser parser =
        CopyInParser.create(
            mock(SessionState.class),
            Format.BINARY,
            null,
            new PipedInputStream(new PipedOutputStream(), 256),
            false);
    assertTrue(parser instanceof BinaryCopyParser);
  }
}
