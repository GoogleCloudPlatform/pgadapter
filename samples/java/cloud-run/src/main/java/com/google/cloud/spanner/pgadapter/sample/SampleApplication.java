// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Sample application for connecting to Cloud Spanner using the PostgreSQL JDBC driver. This sample
 * application starts an embedded in-process PGAdapter instance together with the sample application
 * and connects to it using a Unix domain socket.
 *
 * <p>Modify the properties in application.properties to match your Cloud Spanner database.
 */
@SpringBootApplication
@EnableConfigurationProperties(SpannerProperties.class)
public class SampleApplication {

  private final PGAdapter pgAdapter;

  @Value("${NAME:World}")
  String name;

  public SampleApplication(PGAdapter pgAdapter) {
    this.pgAdapter = pgAdapter;
  }

  @RestController
  class HelloworldController {
    @GetMapping("/")
    String hello() {
      // NOTE: You should use a JDBC connection pool for a production application.
      try (Connection connection = DriverManager.getConnection(pgAdapter.getConnectionUrl())) {
        // Create a prepared statement that takes one query parameter that will be used as the
        // name that will be greeted.
        try (PreparedStatement statement =
            connection.prepareStatement(
                "select 'Hello ' || ? || ' from Cloud Spanner!' as greeting")) {
          statement.setString(1, name);
          try (ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
              return resultSet.getString(1) + "\n";
            } else {
              return "No greeting was returned by Cloud Spanner!\n";
            }
          }
        }
      } catch (Throwable exception) {
        return exception + "\n";
      }
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(SampleApplication.class, args);
  }

  @PreDestroy
  public void onExit() {
    // Stop PGAdapter when the application is shut down.
    pgAdapter.stopPGAdapter();
  }
}
