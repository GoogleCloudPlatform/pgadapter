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
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

public class HibernateConfiguration {

  private Configuration configuration;
  private SessionFactory sessionFactory;

  private HibernateConfiguration(Configuration configuration, SessionFactory sessionFactory) {
    this.configuration = configuration;
    this.sessionFactory = sessionFactory;
  }

  public Session openSession() {
    return sessionFactory.openSession();
  }

  public void closeSessionFactory() {
    sessionFactory.close();
  }

  public static HibernateConfiguration createHibernateConfiguration() {
    final Configuration configuration = new Configuration();
    configuration.addAnnotatedClass(Albums.class);
    configuration.addAnnotatedClass(Concerts.class);
    configuration.addAnnotatedClass(Singers.class);
    configuration.addAnnotatedClass(Venues.class);
    configuration.addAnnotatedClass(Tracks.class);
    configuration.addAnnotatedClass(TracksId.class);

    final SessionFactory sessionFactory =
        configuration.buildSessionFactory(new StandardServiceRegistryBuilder().build());

    return new HibernateConfiguration(configuration, sessionFactory);
  }
}
