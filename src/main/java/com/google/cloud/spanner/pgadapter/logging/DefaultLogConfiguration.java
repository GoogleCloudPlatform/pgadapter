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

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.LogManager;

/** Util class for automatically setting up default logging. */
public class DefaultLogConfiguration {

  public static void configureLogging(String[] args) throws IOException {
    if (shouldUseDefaultLogging(args)) {
      LogManager.getLogManager()
          .readConfiguration(
              DefaultLogConfiguration.class
                  .getClassLoader()
                  .getResourceAsStream("default-logging.properties"));
    }
  }

  static boolean shouldUseDefaultLogging(String[] args) {
    return System.getProperty("java.util.logging.config.file") == null
        && System.getProperty("java.util.logging.config.class") == null
        && (args == null
            || Arrays.stream(args)
                .noneMatch(
                    arg -> arg.equals("--enable_legacy_logging") || arg.equals("-legacy_logging")));
  }

  private DefaultLogConfiguration() {}
}
