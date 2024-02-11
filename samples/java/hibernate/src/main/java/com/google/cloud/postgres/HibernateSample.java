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

package com.google.cloud.postgres;

import com.google.cloud.postgres.models.Albums;
import com.google.cloud.postgres.models.Concerts;
import com.google.cloud.postgres.models.HibernateConfiguration;
import com.google.cloud.postgres.models.Singers;
import com.google.cloud.postgres.models.Tracks;
import com.google.cloud.postgres.models.Venues;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.criteria.Root;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.MutationQuery;
import org.hibernate.query.Query;

public class HibernateSample {

  private static final Logger logger = Logger.getLogger(HibernateSample.class.getName());

  private final HibernateConfiguration hibernateConfiguration;

  private final List<String> singersId = new ArrayList<>();
  private final List<String> albumsId = new ArrayList<>();
  private final List<Long> tracksId = new ArrayList<>();
  private final List<String> venuesId = new ArrayList<>();
  private final List<String> concertsId = new ArrayList<>();

  public HibernateSample(HibernateConfiguration hibernateConfiguration) {
    this.hibernateConfiguration = hibernateConfiguration;
  }

  public void testJPACriteriaDelete() {
    try (Session s = hibernateConfiguration.openSession()) {
      final Singers singers = Utils.createSingers();
      final Albums albums = Utils.createAlbums(singers);
      s.getTransaction().begin();
      s.persist(singers);
      s.persist(albums);
      final Tracks tracks1 = Utils.createTracks(albums.getId());
      s.persist(tracks1);
      final Tracks tracks2 = Utils.createTracks(albums.getId());
      s.persist(tracks2);
      s.getTransaction().commit();
      s.clear();

      CriteriaBuilder cb = s.getCriteriaBuilder();
      CriteriaDelete<Albums> albumsCriteriaDelete = cb.createCriteriaDelete(Albums.class);
      Root<Albums> albumsRoot = albumsCriteriaDelete.from(Albums.class);
      albumsCriteriaDelete.where(cb.equal(albumsRoot.get("id"), albums.getId()));
      Transaction transaction = s.beginTransaction();
      s.createMutationQuery(albumsCriteriaDelete).executeUpdate();
      transaction.commit();
    }
  }

  public void testJPACriteria() {
    try (Session s = hibernateConfiguration.openSession()) {
      CriteriaBuilder cb = s.getCriteriaBuilder();
      CriteriaQuery<Singers> singersCriteriaQuery = cb.createQuery(Singers.class);
      Root<Singers> singersRoot = singersCriteriaQuery.from(Singers.class);
      singersCriteriaQuery
          .select(singersRoot)
          .where(
              cb.and(
                  cb.equal(singersRoot.get("firstName"), "David"),
                  cb.equal(singersRoot.get("lastName"), "Lee")));

      Query<Singers> singersQuery = s.createQuery(singersCriteriaQuery);
      List<Singers> singers = singersQuery.getResultList();

      System.out.println("Listed singer: " + singers.size());

      CriteriaUpdate<Albums> albumsCriteriaUpdate = cb.createCriteriaUpdate(Albums.class);
      Root<Albums> albumsRoot = albumsCriteriaUpdate.from(Albums.class);
      albumsCriteriaUpdate.set("marketingBudget", new BigDecimal("5.0"));
      albumsCriteriaUpdate.where(cb.equal(albumsRoot.get("id"), albumsId.get(0)));
      Transaction transaction = s.beginTransaction();
      s.createMutationQuery(albumsCriteriaUpdate).executeUpdate();
      transaction.commit();
    }
  }

  public void testHqlUpdate() {
    try (Session s = hibernateConfiguration.openSession()) {
      Singers singers = Utils.createSingers();
      singers.setLastName("Cord");
      s.getTransaction().begin();
      s.persist(singers);
      s.getTransaction().commit();

      s.getTransaction().begin();
      MutationQuery query =
          s.createMutationQuery(
              "update Singers set active=:active "
                  + "where lastName=:lastName and firstName=:firstName");
      query.setParameter("active", false);
      query.setParameter("lastName", "Cord");
      query.setParameter("firstName", "David");
      query.executeUpdate();
      s.getTransaction().commit();

      System.out.println("Updated singer: " + s.get(Singers.class, singers.getId()));
    }
  }

  public void testHqlList() {
    try (Session s = hibernateConfiguration.openSession()) {
      Query<Singers> query = s.createQuery("from Singers", Singers.class);
      List<Singers> list = query.list();
      System.out.println("Singers list size: " + list.size());

      query = s.createQuery("from Singers order by fullName", Singers.class);
      query.setFirstResult(2);
      // We must use a limit when we use an offset.
      query.setMaxResults(Integer.MAX_VALUE);
      list = query.list();
      System.out.println("Singers list size with first result: " + list.size());

      query = s.createQuery("from Singers", Singers.class);
      query.setMaxResults(2);
      list = query.list();
      System.out.println("Singers list size with first result: " + list.size());

      Query<Double> sumQuery = s.createQuery("select sum(sampleRate) from Tracks", Double.class);
      System.out.println("Sample rate sum: " + sumQuery.list());
    }
  }

  public void testOneToManyData() {
    try (Session s = hibernateConfiguration.openSession()) {
      Venues venues = s.get(Venues.class, venuesId.get(0));
      if (venues == null) {
        logger.log(Level.SEVERE, "Previously Added Venues Not Found.");
      } else if (venues.getConcerts().size() <= 1) {
        logger.log(Level.SEVERE, "Previously Added Concerts Not Found.");
      }

      System.out.println("Venues fetched: " + venues);
    }
  }

  public void testDeletingData() {
    try (Session s = hibernateConfiguration.openSession()) {
      Singers singers = Utils.createSingers();
      s.getTransaction().begin();
      s.persist(singers);
      s.getTransaction().commit();

      singers = s.get(Singers.class, singers.getId());
      if (singers == null) {
        logger.log(Level.SEVERE, "Added singers not found.");
        return;
      }

      s.getTransaction().begin();
      s.remove(singers);
      s.getTransaction().commit();

      singers = s.get(Singers.class, singers.getId());
      if (singers != null) {
        logger.log(Level.SEVERE, "Deleted singers found.");
      }
    }
  }

  public void testAddingData() {
    try (Session s = hibernateConfiguration.openSession()) {
      final Singers singers = Utils.createSingers();
      final Albums albums = Utils.createAlbums(singers);
      final Venues venues = Utils.createVenue();
      final Concerts concerts1 = Utils.createConcerts(singers, venues);
      final Concerts concerts2 = Utils.createConcerts(singers, venues);
      final Concerts concerts3 = Utils.createConcerts(singers, venues);
      s.getTransaction().begin();
      s.persist(singers);
      s.persist(albums);
      s.persist(venues);
      s.persist(concerts1);
      s.persist(concerts2);
      final Tracks tracks1 = Utils.createTracks(albums.getId());
      s.persist(tracks1);
      final Tracks tracks2 = Utils.createTracks(albums.getId());
      s.persist(tracks2);
      s.persist(concerts3);
      s.getTransaction().commit();

      singersId.add(singers.getId());
      albumsId.add(albums.getId());
      venuesId.add(venues.getId());
      concertsId.add(concerts1.getId());
      concertsId.add(concerts2.getId());
      concertsId.add(concerts3.getId());
      tracksId.add(tracks1.getId().getTrackNumber());
      tracksId.add(tracks2.getId().getTrackNumber());

      System.out.println("Created Singer: " + singers.getId());
      System.out.println("Created Albums: " + albums.getId());
      System.out.println("Created Venues: " + venues.getId());
      System.out.println("Created Concerts: " + concerts1.getId());
      System.out.println("Created Concerts: " + concerts2.getId());
      System.out.println("Created Concerts: " + concerts3.getId());
      System.out.println("Created Tracks: " + tracks1.getId());
      System.out.println("Created Tracks: " + tracks2.getId());
    }
  }

  public void testSessionRollback() {
    try (Session s = hibernateConfiguration.openSession()) {
      final Singers singers = Utils.createSingers();
      s.getTransaction().begin();
      s.persist(singers);
      s.getTransaction().rollback();

      System.out.println("Singers that was saved: " + singers.getId());
      Singers singersFromDb = s.get(Singers.class, singers.getId());
      if (singersFromDb == null) {
        System.out.println("Singers not found as expected.");
      } else {
        logger.log(Level.SEVERE, "Singers found. Lookout for the error.");
      }
    }
  }

  public void testForeignKey() {
    try (Session s = hibernateConfiguration.openSession()) {
      final Singers singers = Utils.createSingers();
      final Albums albums = Utils.createAlbums(singers);

      s.getTransaction().begin();
      s.persist(singers);
      s.persist(albums);
      s.getTransaction().commit();

      singersId.add(singers.getId());
      albumsId.add(albums.getId());
      System.out.println("Created Singer: " + singers.getId());
      System.out.println("Created Albums: " + albums.getId());
    }
  }

  public void runHibernateSample() {
    try {
      System.out.println("Testing Foreign Key");
      testForeignKey();
      System.out.println("Foreign Key Test Completed");

      System.out.println("Testing Session Rollback");
      testSessionRollback();
      System.out.println("Session Rollback Test Completed");

      System.out.println("Testing Data Insert");
      testAddingData();
      System.out.println("Data Insert Test Completed");

      System.out.println("Testing Data Delete");
      testDeletingData();
      System.out.println("Data Delete Test Completed");

      System.out.println("Testing One to Many Fetch");
      testOneToManyData();
      System.out.println("One To Many Fetch Test Completed");

      System.out.println("Testing HQL List");
      testHqlList();
      System.out.println("HQL List Test Completed");

      System.out.println("Testing HQL Update");
      testHqlUpdate();
      System.out.println("HQL Update Test Completed");

      System.out.println("Testing JPA List and Update");
      testJPACriteria();
      System.out.println("JPA List and Update Test Completed");

      System.out.println("Testing JPA Delete");
      testJPACriteriaDelete();
      System.out.println("JPA Delete Test Completed");
    } finally {
      // Make sure we always close the session factory when the test is done. Otherwise, the sample
      // application might keep non-daemon threads alive and not stop.
      hibernateConfiguration.closeSessionFactory();
    }
  }
}
