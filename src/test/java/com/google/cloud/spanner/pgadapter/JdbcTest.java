package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcTest {

  @BeforeClass
  public static void loadDriver() throws ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
  }

  @Test
  public void testJdbc() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/knut-test-db", "loite", "loite")) {
      connection.createStatement().execute("delete from test");

      try (PreparedStatement preparedStatement = connection.prepareStatement("insert into test (key, value) values (?,?) returning key")) {
        preparedStatement.setInt(1, 1);
        preparedStatement.setString(2, "One");
        SQLException exception = assertThrows(SQLException.class, preparedStatement::executeUpdate);
        assertEquals("A result was returned when none was expected.", exception.getMessage());
      }
      assertEquals(1, connection.createStatement().executeUpdate("delete from test"));

      connection.setAutoCommit(false);
      try (PreparedStatement preparedStatement = connection.prepareStatement("insert into test (key, value) values (?,?) returning key")) {
        preparedStatement.setInt(1, 1);
        preparedStatement.setString(2, "One");
        SQLException exception = assertThrows(SQLException.class, preparedStatement::executeUpdate);
        assertEquals("A result was returned when none was expected.", exception.getMessage());
      }
      assertEquals(1, connection.createStatement().executeUpdate("delete from test"));
      connection.commit();
    }
  }

}
