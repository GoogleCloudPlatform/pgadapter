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

package com.google.cloud.spanner.pgadapter.logging;

import com.google.common.collect.ImmutableList;
import java.io.OutputStream;
import java.util.List;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

public class StdoutHandler extends StreamHandler {
  public StdoutHandler() {
    super(System.out, new SimpleFormatter());
  }

  @Override
  public void publish(LogRecord record) {
    if (record.getLevel().intValue() <= Level.INFO.intValue()) {
      super.publish(record);
      flush();
    }
  }

  @Override
  public void close() {
    flush();
  }

}
