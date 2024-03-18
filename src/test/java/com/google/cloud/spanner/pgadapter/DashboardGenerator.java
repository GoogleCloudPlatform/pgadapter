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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type.StructField;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
    String output = original.toString().replace("{{ dashboard.contents }}", readYcsbResults());
    try (FileWriter writer = new FileWriter(templateFile)) {
      writer.write(output);
      writer.flush();
    }
  }

  private static String readYcsbResults() {
    String results = "";
    try (Spanner spanner = SpannerOptions.newBuilder().build().getService()) {
      DatabaseClient client =
          spanner.getDatabaseClient(
              DatabaseId.of(
                  spanner.getOptions().getProjectId(),
                  "pgadapter-ycsb-regional-test",
                  "pgadapter-ycsb-test"));
      results = results + "\n\n" + "### Workload A\n\n";
      try (ResultSet resultSetWorkloadA =
          client
              .singleUse()
              .executeQuery(
                  Statement.of(
                      "select tcp.workload, tcp.executed_at, "
                          + "tcp.read_p95 as tcp_read_p95, uds.read_p95 as uds_read_p95, "
                          + "tcp.update_p95 as tcp_update_p95, uds.update_p95 as uds_update_p95\n"
                          + "from run tcp\n"
                          + "inner join run uds using (executed_at, threads, batch_size, workload)\n"
                          + "where tcp.deployment='java_tcp'\n"
                          + "and uds.deployment='java_uds'\n"
                          + "and tcp.threads=50 and tcp.batch_size=50\n"
                          + "and tcp.workload='f'\n"
                          + "order by executed_at"))) {
        results = results + convertResultSetToMarkdown(resultSetWorkloadA);
      }

      results = results + "\n\n" + "### Workload D\n\n";
      try (ResultSet resultSetWorkloadD =
          client
              .singleUse()
              .executeQuery(
                  Statement.of(
                      "select tcp.workload, tcp.executed_at, "
                          + "tcp.read_p95 as tcp_read_p95, uds.read_p95 as uds_read_p95, "
                          + "tcp.insert_p95 as tcp_insert_p95, uds.insert_p95 as uds_insert_p95\n"
                          + "from run tcp\n"
                          + "inner join run uds using (executed_at, threads, batch_size, workload)\n"
                          + "where tcp.deployment='java_tcp'\n"
                          + "and uds.deployment='java_uds'\n"
                          + "and tcp.threads=50 and tcp.batch_size=50\n"
                          + "and tcp.workload='d'\n"
                          + "order by executed_at"))) {
        results = results + convertResultSetToMarkdown(resultSetWorkloadD);
      }
    }
    return results;
  }

  private static String convertResultSetToMarkdown(ResultSet resultSet) {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    while (resultSet.next()) {
      if (first) {
        for (StructField header : resultSet.getType().getStructFields()) {
          builder.append("| ").append(header.getName());
        }
        builder.append('\n');
        for (StructField ignored : resultSet.getType().getStructFields()) {
          builder.append("|-");
        }
        builder.append('\n');
        first = false;
      }
      for (int i = 0; i < resultSet.getColumnCount(); i++) {
        builder.append("| ").append(resultSet.getValue(i).toString());
      }
      builder.append('\n');
    }
    return builder.toString();
  }
}
