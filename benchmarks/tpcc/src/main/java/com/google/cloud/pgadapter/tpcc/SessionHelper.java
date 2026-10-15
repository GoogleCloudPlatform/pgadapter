package com.google.cloud.pgadapter.tpcc;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class SessionHelper {

  private final SessionFactory sessionFactory;

  public SessionHelper(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public Session createSession(boolean readOnly, boolean autoBatchDml, String transactionTag) {
    if (readOnly) return createReadOnlySession();
    if (autoBatchDml) return createAutoBatchDmlSession(transactionTag);
    return createReadWriteSession(transactionTag);
  }

  public Session createReadWriteSession(String transactionTag) {
    Session session = sessionFactory.openSession();
    session.doWork(
        conn -> {
          conn.createStatement().execute("RESET ALL");
          conn.setReadOnly(false);
          //conn.createStatement().execute("set transaction_tag='" + transactionTag + "'");
        });
    return session;
  }

  public Session createAutoBatchDmlSession(String transactionTag) {
    Session session = sessionFactory.openSession();
    session.doWork(
        conn -> {
          conn.createStatement().execute("RESET ALL");
          conn.setReadOnly(false);
          conn.createStatement().execute("set auto_batch_dml=true");
          conn.createStatement().execute("set auto_batch_dml_update_count_verification=false");
          //conn.createStatement().execute("set transaction_tag='" + transactionTag + "'");
        });
    return session;
  }

  public Session createReadOnlySession() {
    Session session = sessionFactory.openSession();
    session.doWork(
        conn -> {
          conn.createStatement().execute("RESET ALL");
          conn.setReadOnly(true);
        });
    return session;
  }
}
