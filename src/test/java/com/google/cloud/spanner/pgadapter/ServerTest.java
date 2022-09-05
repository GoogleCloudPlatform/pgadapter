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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ServerTest {

  @Test
  public void testExtractMetadata() {
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(byteArrayStream);
    String expectedPGAdapterVersion = Server.getVersion();
    String expectedPostgreSQLVersion = "14.1";

    Server.extractMetadata(new String[] {}, out);

    assertEquals(
        "-- Starting PGAdapter version "
            + expectedPGAdapterVersion
            + " --\n"
            + "-- PostgreSQL version: "
            + expectedPostgreSQLVersion
            + " -- \n",
        byteArrayStream.toString());
  }

  @Test
  public void testPrintError() {
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(byteArrayStream);
    Exception exception = new Exception("test exception");
    String expectedPGAdapterVersion = Server.getVersion();

    Server.printError(exception, out, out);

    assertEquals(
        "The server could not be started because an error occurred: "
            + exception.getMessage()
            + "\n"
            + "Run with option -h or --help to get help\n"
            + "Version: "
            + expectedPGAdapterVersion
            + "\n",
        byteArrayStream.toString());
  }

  @Test
  public void testMainWithInvalidParam() {
    ByteArrayOutputStream outArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outArrayStream);
    ByteArrayOutputStream errArrayStream = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errArrayStream);

    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    System.setOut(out);
    System.setErr(err);

    try {
      Server.main(new String[] {"--invalid-param"});
      assertEquals(
          "The server could not be started because an error occurred: Unrecognized option: --invalid-param\n",
          errArrayStream.toString());
      assertTrue(
          outArrayStream.toString(),
          outArrayStream
              .toString()
              .startsWith(
                  String.format("-- Starting PGAdapter version %s --", Server.getVersion())));
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }
  }
}
