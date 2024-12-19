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

import static com.google.cloud.spanner.pgadapter.logging.DefaultLogConfiguration.shouldUseDefaultLogging;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DefaultLogConfigurationTest {

  @Test
  public void testShouldUseDefaultLogging() {
    assumeTrue(System.getProperty("java.util.logging.config.file") == null);
    assumeTrue(System.getProperty("java.util.logging.config.class") == null);

    assertTrue(shouldUseDefaultLogging(null));
    assertTrue(shouldUseDefaultLogging(new String[0]));
    assertTrue(shouldUseDefaultLogging(new String[] {"foo"}));

    assertFalse(shouldUseDefaultLogging(new String[] {"-legacy_logging"}));
    assertFalse(shouldUseDefaultLogging(new String[] {"--enable_legacy_logging"}));
    runWithSystemProperty(
        "java.util.logging.config.file",
        "foo",
        () -> assertFalse(shouldUseDefaultLogging(new String[0])));
    runWithSystemProperty(
        "java.util.logging.config.class",
        "TestClass",
        () -> assertFalse(shouldUseDefaultLogging(new String[0])));
  }

  void runWithSystemProperty(String key, String value, Runnable runnable) {
    String original = System.getProperty(key);
    try {
      System.setProperty(key, value);
      runnable.run();
    } finally {
      if (original == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, original);
      }
    }
  }
}
