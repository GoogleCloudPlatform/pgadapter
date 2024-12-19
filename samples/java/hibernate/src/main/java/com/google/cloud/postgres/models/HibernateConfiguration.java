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

package com.google.cloud.postgres.models;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateConfiguration {
  private final SessionFactory sessionFactory;

  private HibernateConfiguration(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public Session openSession() {
    return sessionFactory.openSession();
  }

  public void closeSessionFactory() {
    sessionFactory.close();
  }

  public static HibernateConfiguration createHibernateConfiguration(String connectionUrl) {
    final Configuration configuration = new Configuration();

    configuration.setProperty("hibernate.connection.url", connectionUrl);
    configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
    configuration.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    configuration.setProperty("hibernate.show_sql", "true");
    configuration.setProperty("hibernate.format_sql", "true");
    configuration.setProperty("hibernate.connection.pool_size", "5");
    configuration.setProperty("hibernate.hbm2ddl.auto", "none");

    configuration.addAnnotatedClass(Albums.class);
    configuration.addAnnotatedClass(Concerts.class);
    configuration.addAnnotatedClass(Singers.class);
    configuration.addAnnotatedClass(Venues.class);
    configuration.addAnnotatedClass(Tracks.class);
    configuration.addAnnotatedClass(TracksId.class);
    configuration.addAnnotatedClass(TicketSale.class);

    final SessionFactory sessionFactory = configuration.buildSessionFactory();

    return new HibernateConfiguration(sessionFactory);
  }
}
