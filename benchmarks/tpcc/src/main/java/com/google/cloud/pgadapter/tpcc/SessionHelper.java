package com.google.cloud.pgadapter.tpcc;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class SessionHelper {

  private final SessionFactory sessionFactory;

  public SessionHelper(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public Session createSession(boolean readOnly, boolean autoBatchDml) {
    if (readOnly) return createReadOnlySession();
    if (autoBatchDml) return createAutoBatchDmlSession();
    return createReadWriteSession();
  }

  public Session createReadWriteSession() {
    Session session = sessionFactory.openSession();
    session.doWork(
        conn -> {
          conn.setReadOnly(false);
          conn.createStatement().execute("set auto_batch_dml=false");
          conn.createStatement().execute("set auto_batch_dml_update_count_verification=true");
        });
    return session;
  }

  public Session createAutoBatchDmlSession() {
    Session session = sessionFactory.openSession();
    session.doWork(
        conn -> {
          conn.setReadOnly(false);
          conn.createStatement().execute("set auto_batch_dml=true");
          conn.createStatement().execute("set auto_batch_dml_update_count_verification=false");
        });
    return session;
  }

  public Session createReadOnlySession() {
    Session session = sessionFactory.openSession();
    session.doWork(
        conn -> {
          conn.setReadOnly(true);
          conn.createStatement().execute("set auto_batch_dml=false");
          conn.createStatement().execute("set auto_batch_dml_update_count_verification=false");
        });
    return session;
  }
}
