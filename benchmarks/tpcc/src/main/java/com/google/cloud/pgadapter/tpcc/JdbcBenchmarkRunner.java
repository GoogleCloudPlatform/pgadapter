package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcBenchmarkRunner extends AbstractBenchmarkRunner {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcBenchmarkRunner.class);

  JdbcBenchmarkRunner(
      Statistics statistics, String connectionUrl, TpccConfiguration tpccConfiguration) {
    super(statistics, connectionUrl, tpccConfiguration);
  }

  Object[] queryRow(
      QueryRowMode queryRowMode, Statement statement, String query, Object... parameters)
      throws SQLException {
    String sql = String.format(query, parameters);
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      if (!resultSet.next()) {
        if (queryRowMode == QueryRowMode.ALLOW_LESS_THAN_ONE) {
          return null;
        } else {
          throw new RowNotFoundException(String.format("No results found for: %s", sql));
        }
      }
      Object[] result = new Object[resultSet.getMetaData().getColumnCount()];
      for (int i = 0; i < result.length; i++) {
        result[i] = resultSet.getObject(i + 1);
      }
      if (queryRowMode != QueryRowMode.ALLOW_MORE_THAN_ONE && resultSet.next()) {
        throw new SQLException(String.format("More than one result found for: %s", sql));
      }
      return result;
    }
  }
}
