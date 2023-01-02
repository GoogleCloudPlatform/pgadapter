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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;

public class DashboardGenerator {

  public static void main(String[] args) throws IOException {
    String templateFile = ".github/test-template.md";
    StringBuilder original = new StringBuilder();
    try (Scanner scanner = new Scanner(new FileReader(templateFile))) {
      while (scanner.hasNextLine()) {
        original.append(scanner.nextLine()).append("\n");
      }
    }
    String output =
        original
            .toString()
            .replace(
                "{{ dashboard.contents }}",
                "This is some random content: " + new Random().nextInt());
    try (FileWriter writer = new FileWriter(templateFile)) {
      writer.write(output);
      writer.flush();
    }
  }

  private static void readYcsbResults() throws IOException {
    try (Spanner spanner = SpannerOptions.newBuilder()
        .setCredentials(GoogleCredentials.fromStream(
            Files.newInputStream(Paths.get("/Users/loite/Downloads/cloud-spanner-pg-adapter.json"))))
        .build().getService()) {
      DatabaseClient client = spanner.getDatabaseClient(
          DatabaseId.of(
              spanner.getOptions().getProjectId(),
              "pgadapter-ycsb-regional-test",
              "pgadapter-ycsb-test"
              ));
      String lastExecutionTime;
      try (ResultSet resultSet = client.singleUse().executeQuery(Statement.of("select max(executed_at) from run"))) {
        while (resultSet.next()) {
          lastExecutionTime = resultSet.getString(0);
        }
      }
      
    }
  }

}
