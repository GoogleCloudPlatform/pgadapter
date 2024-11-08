package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.pgadapter.tpcc.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.tpcc.config.SpannerConfiguration;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.pgadapter.tpcc.entities.District;
import com.google.cloud.pgadapter.tpcc.entities.DistrictId;
import com.google.cloud.spanner.Dialect;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

public class HibernateBenchmarkRunner extends AbstractBenchmarkRunner {

  private final SessionFactory sessionFactory; // SessionFactory injected
  private final ThreadLocal<Session> sessionThreadLocal = new ThreadLocal<>();

  HibernateBenchmarkRunner(
      Statistics statistics,
      SessionFactory sessionFactory, // Injected SessionFactory
      TpccConfiguration tpccConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      SpannerConfiguration spannerConfiguration,
      Metrics metrics,
      Dialect dialect) {
    super(
        statistics,
        tpccConfiguration,
        pgAdapterConfiguration,
        spannerConfiguration,
        metrics,
        dialect);
    this.sessionFactory = sessionFactory;
  }

  @Override
  void setup() throws SQLException, IOException {
    sessionThreadLocal.set(sessionFactory.openSession()); // Open session here
  }

  @Override
  void teardown() throws SQLException {
    Session session = sessionThreadLocal.get();
    if (session != null) {
      session.close();
      sessionThreadLocal.remove();
    }
  }

  @Override
  public void newOrder() throws SQLException {
    Session session = sessionThreadLocal.get();
    Transaction tx = session.beginTransaction();

    // Execute the "select 1" query
    try {
      District district = session.get(District.class, new DistrictId(0l, 0l));
      System.out.println(district);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    tx.commit();
  }

  @Override
  public void payment() throws SQLException {
    Session session = sessionThreadLocal.get();
    Transaction tx = session.beginTransaction();

    // Execute the "select 1" query
    try {
      Integer result = session.createSelectionQuery("select 2", Integer.class).getSingleResult();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    tx.commit();
  }

  @Override
  public void orderStatus() throws SQLException {
    Session session = sessionThreadLocal.get();
    Transaction tx = session.beginTransaction();

    // Execute the "select 1" query
    try {
      Integer result = session.createSelectionQuery("select 3", Integer.class).getSingleResult();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    tx.commit();
  }

  @Override
  public void delivery() throws SQLException {
    Session session = sessionThreadLocal.get();
    Transaction tx = session.beginTransaction();

    // Execute the "select 1" query
    try {
      Integer result = session.createSelectionQuery("select 4", Integer.class).getSingleResult();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    tx.commit();
  }

  public void stockLevel() throws SQLException {
    Session session = sessionThreadLocal.get();
    Transaction tx = session.beginTransaction();

    // Execute the "select 1" query
    try {
      Integer result = session.createSelectionQuery("select 5", Integer.class).getSingleResult();
    } catch (Exception e) {
    }
    tx.commit();
  }

  @Override
  void executeStatement(String sql) throws SQLException {}

  @Override
  Object[] paramQueryRow(String sql, Object[] params) throws SQLException {
    return new Object[0];
  }

  @Override
  void executeParamStatement(String sql, Object[] params) throws SQLException {}

  @Override
  List<Object[]> executeParamQuery(String sql, Object[] params) throws SQLException {
    return null;
  }
}
