// Copyright 2020 Google LLC
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

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;

/** Effectively this is the main class */
public class Server {

  /**
   * Main method for running a Spanner PostgreSQL Adaptor {@link Server} as a stand-alone
   * application. Here we call for parameter parsing and start the Proxy Server.
   */
  public static void main(String[] args) {
    System.out.println("Starting PGAdapter with batching support");
    try {
      OptionsMetadata optionsMetadata = new OptionsMetadata(args);
      ProxyServer server = new ProxyServer(optionsMetadata);
      server.startServer();
    } catch (Exception e) {
      System.err.println(
          "The server could not be started because an error occurred: "
              + (e.getMessage() == null ? e.toString() : e.getMessage()));
      System.out.println("Run with option -h or --help to get help");
    }
  }
}
