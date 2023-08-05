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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Sample application for connecting to Cloud Spanner using the PostgreSQL JDBC driver on Cloud Run.
 * This sample application runs PGAdapter as a sidecar container on Cloud Run.
 *
 * <p>Modify service.yaml to match your Cloud Run project and region, and your Cloud Spanner database.
 */
@SpringBootApplication
public class SampleApplication {

  @Value("${NAME:World}")
  String name;

  @RestController
  class HelloworldController {
    @GetMapping("/")
    String hello() {
      String connectionUrl;
      if (System.getProperty("connection_url") != null) {
        // This system property is set in the pom.xml file and is used when running locally with the
        // `mvn spring-boot:run` command.
        connectionUrl = System.getProperty("connection_url");
      } else {
        // Connect to PGAdapter using Unix Domain Sockets. This gives you the lowest possible
        // latency. The PGAdapter sidecar container and the main container both share the /sockets
        // directory, and PGAdapter is instructed to use this directory for Unix domain sockets.
        connectionUrl = String.format("jdbc:postgresql://localhost/?"
            + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
            + "&socketFactoryArg=/sockets/.s.PGSQL.%d", 5432);
      }
      // NOTE: You should use a JDBC connection pool for a production application.
      try (Connection connection = DriverManager.getConnection(connectionUrl)) {
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
}
