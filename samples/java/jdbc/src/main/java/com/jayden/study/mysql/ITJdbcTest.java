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

package com.jayden.study.mysql;

import com.google.cloud.Timestamp;
import com.jayden.study.utils.JdbcUtils;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class ITJdbcTest {

  private static final String url = "jdbc:mysql://localhost:3306/test?autoReconnect=true&useSSL=false";
  private static final String user = "pratick";
  private static final String password = "password";

  private static Connection connection;
  private static Random random = new Random();


  public static void main(String[] args) {
    try {
      ITJdbcTest itJdbcTest = new ITJdbcTest();
      connection = JdbcUtils.getConnection(url, user, password);

      itJdbcTest.testSelectHelloWorld();
      itJdbcTest.insertTestData();
      // itJdbcTest.testCreateTableIfNotExists();
      itJdbcTest.testSelectWithParameters();
      itJdbcTest.testUpdateWithParameters();
    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      JdbcUtils.closeConnection(connection);
    }
  }

  public void insertTestData() {

    try {
      int input = random.nextInt();
      connection.createStatement().execute(
          "insert into Singers (created_at, first_name, last_name, updated_at, id) values ('2023-01-17 19:12:35.664096', 'David', 'Lee', '2023-01-17 19:12:35.674549', '"
              + UUID.randomUUID().toString() +
              "')");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void testSelectHelloWorld() throws SQLException {
    try (ResultSet resultSet =
        connection.createStatement().executeQuery("SELECT 'Hello World!'")) {
      resultSet.next();
    }
  }

  public void testSelectWithParameters() throws SQLException {
    boolean isSimpleMode = false;
    String sql =
        "select singers_.full_name as full_nam4_2_ from Singers singers_ where singers_.id='f11cb732-8bc6-45a7-b04b-709c60f56f59'";

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      try (ResultSet resultSet = statement.executeQuery()) {
      }
    }
  }

  public void testUpdateWithParameters() throws SQLException {
    boolean isSimpleMode = false;
    String sql =
        "update Singers set created_at='2023-01-17 19:15:54.586221' where last_name='Cord' and first_name='David'";

    try (PreparedStatement statement = connection.prepareStatement(sql)) {

      statement.executeUpdate();
    }
  }
}
