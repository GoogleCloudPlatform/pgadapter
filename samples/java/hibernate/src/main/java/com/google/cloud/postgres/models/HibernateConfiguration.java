package com.google.cloud.postgres.models;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

public class HibernateConfiguration {

  private Configuration configuration;
  private SessionFactory sessionFactory;

  private HibernateConfiguration(Configuration configuration,
      SessionFactory sessionFactory) {
    this.configuration = configuration;
    this.sessionFactory = sessionFactory;
  }

  public Session openSession() {
    return sessionFactory.openSession();
  }

  public static HibernateConfiguration createHibernateConfiguration() {
    final Configuration configuration = new Configuration();
    configuration.addAnnotatedClass(Albums.class);
    configuration.addAnnotatedClass(Concerts.class);
    configuration.addAnnotatedClass(Singers.class);
    configuration.addAnnotatedClass(Venues.class);
    configuration.addAnnotatedClass(Tracks.class);
    configuration.addAnnotatedClass(TracksId.class);

    final SessionFactory sessionFactory = configuration.buildSessionFactory(
        new StandardServiceRegistryBuilder().build());

    return new HibernateConfiguration(configuration, sessionFactory);
  }

}
