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

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/** {@link StreamHandler} that automatically redirects output to either stdout or stderr based on the log record level. */
public class PGAdapterConsoleHandler extends StreamHandler {
  private static final Level MAX_STDERR_LEVEL = Level.WARNING;

  public static void configureDefaultLogging() {
    if (System.getProperty("java.util.logging.config.file") != null) {
      return;
    }
    System.setProperty("", "");
  }

  private final ConsoleHandler stderrConsoleHandler;

  public PGAdapterConsoleHandler() {
    super(System.out, new SimpleFormatter());
    this.stderrConsoleHandler = new ConsoleHandler();
  }

  @Override
  public void publish(LogRecord record) {
    super.publish(record);
    flush();
  }

  @Override
  public void close() {
    flush();
  }

}
