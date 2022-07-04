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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser.CopyOptions;
import java.io.IOException;
import java.io.PipedOutputStream;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.postgresql.jdbc.TimestampUtils;

interface CopyInParser {
  static CopyInParser create(
      CopyOptions.Format format,
      CSVFormat csvFormat,
      PipedOutputStream payload,
      int pipeBufferSize,
      boolean hasHeader)
      throws IOException {
    switch (format) {
      case TEXT:
      case CSV:
        return new CsvCopyParser(csvFormat, payload, pipeBufferSize, hasHeader);
      case BINARY:
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Unsupported COPY format: " + format);
    }
  }

  static TimestampUtils createDefaultTimestampUtils() {
    return new TimestampUtils(false, () -> null);
  }

  /** Returns an iterator of COPY records. */
  Iterator<CopyRecord> iterator();

  /** Closes this parser and releases any resources associated with it. */
  void close() throws IOException;
}
