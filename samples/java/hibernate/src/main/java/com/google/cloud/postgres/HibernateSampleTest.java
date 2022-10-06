package com.google.cloud.postgres;

import com.google.cloud.postgres.models.Albums;
import com.google.cloud.postgres.models.Concerts;
import com.google.cloud.postgres.models.HibernateConfiguration;
import com.google.cloud.postgres.models.Singers;
import com.google.cloud.postgres.models.Tracks;
import com.google.cloud.postgres.models.Venues;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.Root;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

public class HibernateSampleTest {

  private static final Logger logger = Logger.getLogger(HibernateSampleTest.class.getName());

  private HibernateConfiguration hibernateConfiguration;

  private List<String> singersId = new ArrayList<>();
  private List<String> albumsId = new ArrayList<>();
  private List<Long> tracksId = new ArrayList<>();
  private List<String> venuesId = new ArrayList<>();
  private List<String> concertsId = new ArrayList<>();

  public HibernateSampleTest(
      HibernateConfiguration hibernateConfiguration) {
    this.hibernateConfiguration = hibernateConfiguration;
  }

  public void testJPACriteriaDelete() {
    Session s = hibernateConfiguration.openSession();

    final Singers singers = Utils.createSingers();
    final Albums albums = Utils.createAlbums(singers);
    s.getTransaction().begin();
    s.saveOrUpdate(singers);
    s.saveOrUpdate(albums);
    final Tracks tracks1 = Utils.createTracks(albums.getId());
    s.saveOrUpdate(tracks1);
    final Tracks tracks2 = Utils.createTracks(albums.getId());
    s.saveOrUpdate(tracks2);
    s.getTransaction().commit();
    s.clear();

    CriteriaBuilder cb = s.getCriteriaBuilder();
    CriteriaDelete<Albums> albumsCriteriaDelete = cb.createCriteriaDelete(Albums.class);
    Root<Albums> albumsRoot = albumsCriteriaDelete.from(Albums.class);
    albumsCriteriaDelete.where(cb.equal(albumsRoot.get("id"), albums.getId()));
    Transaction transaction = s.beginTransaction();
    s.createQuery(albumsCriteriaDelete).executeUpdate();
    transaction.commit();

    s.close();
  }

  public void testJPACriteria() {
    Session s = hibernateConfiguration.openSession();
    CriteriaBuilder cb = s.getCriteriaBuilder();
    CriteriaQuery<Singers> singersCriteriaQuery = cb.createQuery(Singers.class);
    Root<Singers> singersRoot = singersCriteriaQuery.from(Singers.class);
    singersCriteriaQuery
        .select(singersRoot)
        .where(
            cb.and(cb.equal(singersRoot.get("firstName"), "David"),
            cb.equal(singersRoot.get("lastName"), "Lee")));

    Query<Singers> singersQuery = s.createQuery(singersCriteriaQuery);
    List<Singers> singers = singersQuery.getResultList();

    logger.log(Level.INFO, "Listed singer: {0}", singers.size());

    CriteriaUpdate<Albums> albumsCriteriaUpdate = cb.createCriteriaUpdate(Albums.class);
    Root<Albums> albumsRoot = albumsCriteriaUpdate.from(Albums.class);
    albumsCriteriaUpdate.set("marketingBudget", new BigDecimal("5.0"));
    albumsCriteriaUpdate.where(cb.equal(albumsRoot.get("id"), UUID.fromString(albumsId.get(0))));
    Transaction transaction = s.beginTransaction();
    s.createQuery(albumsCriteriaUpdate).executeUpdate();
    transaction.commit();

    s.close();
  }

  public void testHqlUpdate() {
    Session s = hibernateConfiguration.openSession();

    Singers singers = Utils.createSingers();
    singers.setLastName("Cord");
    s.getTransaction().begin();
    s.saveOrUpdate(singers);
    s.getTransaction().commit();

    s.getTransaction().begin();
    Query query = s.createQuery(
        "update Singers set active=:active "
            + "where lastName=:lastName and firstName=:firstName");
    query.setParameter("active", false);
    query.setParameter("lastName", "Cord");
    query.setParameter("firstName", "David");
    query.executeUpdate();
    s.getTransaction().commit();

    logger.log(Level.INFO, "Updated singer: {0}", s.get(Singers.class, singers.getId()));
    s.close();
  }

  public void testHqlList() {
    Session s = hibernateConfiguration.openSession();

    Query query = s.createQuery("from Singers");
    List<Singers> list = query.list();
    logger.log(Level.INFO, "Singers list size: {0}", list.size());

    query = s.createQuery("from Singers order by fullName");
    query.setFirstResult(2);
    list = query.list();
    logger.log(Level.INFO, "Singers list size with first result: {0}", list.size());

    /* Current Limit is not supported. */
    // query = s.createQuery("from Singers");
    // query.setMaxResults(2);
    // list = query.list();
    // logger.log(Level.INFO, "Singers list size with first result: {0}", list.size());

    query = s.createQuery("select  sum(sampleRate) from Tracks");
    list = query.list();
    logger.log(Level.INFO, "Sample rate sum: {0}", list);
    s.close();
  }

  public void testOneToManyData() {
    Session s = hibernateConfiguration.openSession();

    Venues venues = s.get(Venues.class, UUID.fromString(venuesId.get(0)));
    if (venues == null) {
      logger.log(Level.SEVERE, "Previously Added Venues Not Found.");
    }
    if (venues.getConcerts().size() <= 1) {
      logger.log(Level.SEVERE, "Previously Added Concerts Not Found.");
    }

    logger.log(Level.INFO, "Venues fetched: {0}", venues);
    s.close();
  }

  public void testDeletingData() {
    Session s = hibernateConfiguration.openSession();

    Singers singers = Utils.createSingers();
    s.getTransaction().begin();
    s.saveOrUpdate(singers);
    s.getTransaction().commit();

    singers = s.get(Singers.class, singers.getId());
    if (singers == null) {
      logger.log(Level.SEVERE, "Added singers not found.");
    }

    s.getTransaction().begin();
    s.delete(singers);
    s.getTransaction().commit();

    singers = s.get(Singers.class, singers.getId());
    if (singers != null) {
      logger.log(Level.SEVERE, "Deleted singers found.");
    }
    s.close();
  }

  public void testAddingData() {
    Session s = hibernateConfiguration.openSession();
    final Singers singers = Utils.createSingers();
    final Albums albums = Utils.createAlbums(singers);
    final Venues venues = Utils.createVenue();
    final Concerts concerts1 = Utils.createConcerts(singers, venues);
    final Concerts concerts2 = Utils.createConcerts(singers, venues);
    final Concerts concerts3 = Utils.createConcerts(singers, venues);
    s.getTransaction().begin();
    s.saveOrUpdate(singers);
    s.saveOrUpdate(albums);
    s.saveOrUpdate(venues);
    s.persist(concerts1);
    s.persist(concerts2);
    final Tracks tracks1 = Utils.createTracks(albums.getId());
    s.saveOrUpdate(tracks1);
    final Tracks tracks2 = Utils.createTracks(albums.getId());
    s.saveOrUpdate(tracks2);
    s.persist(concerts3);
    s.getTransaction().commit();

    singersId.add(singers.getId().toString());
    albumsId.add(albums.getId().toString());
    venuesId.add(venues.getId().toString());
    concertsId.add(concerts1.getId().toString());
    concertsId.add(concerts2.getId().toString());
    concertsId.add(concerts3.getId().toString());
    tracksId.add(tracks1.getId().getTrackNumber());
    tracksId.add(tracks2.getId().getTrackNumber());

    logger.log(Level.INFO, "Created Singer: {0}", singers.getId());
    logger.log(Level.INFO, "Created Albums: {0}", albums.getId());
    logger.log(Level.INFO, "Created Venues: {0}", venues.getId());
    logger.log(Level.INFO, "Created Concerts: {0}", concerts1.getId());
    logger.log(Level.INFO, "Created Concerts: {0}", concerts2.getId());
    logger.log(Level.INFO, "Created Concerts: {0}", concerts3.getId());
    logger.log(Level.INFO, "Created Tracks: {0}", tracks1.getId());
    logger.log(Level.INFO, "Created Tracks: {0}", tracks2.getId());
    s.close();
  }

  public void testSessionRollback() {
    Session s = hibernateConfiguration.openSession();
    final Singers singers = Utils.createSingers();
    s.getTransaction().begin();
    s.saveOrUpdate(singers);
    s.getTransaction().rollback();

    logger.log(Level.INFO, "Singers that was saved: ", singers.getId());
    Singers singersFromDb = s.get(Singers.class, singers.getId());
    if (singersFromDb == null) {
      logger.log(Level.INFO, "Singers not found as expected.");
    } else {
      logger.log(Level.SEVERE, "Singers found. Lookout for the error.");
    }
    s.close();
  }

  public void testForeignKey() {
    Session s = hibernateConfiguration.openSession();

    final Singers singers = Utils.createSingers();
    final Albums albums = Utils.createAlbums(singers);

    s.getTransaction().begin();
    s.saveOrUpdate(singers);
    s.persist(albums);
    s.getTransaction().commit();

    singersId.add(singers.getId().toString());
    albumsId.add(albums.getId().toString());
    logger.log(Level.INFO, "Created Singer: {0}", singers.getId());
    logger.log(Level.INFO, "Created Albums: {0}", albums.getId());
    s.close();
  }

  public void executeTest() {
    logger.log(Level.INFO, "Testing Foreign Key");
    testForeignKey();
    logger.log(Level.INFO, "Foreign Key Test Completed");

    logger.log(Level.INFO, "Testing Session Rollback");
    testSessionRollback();
    logger.log(Level.INFO, "Session Rollback Test Completed");

    logger.log(Level.INFO, "Testing Data Insert");
    testAddingData();
    logger.log(Level.INFO, "Data Insert Test Completed");

    logger.log(Level.INFO, "Testing Data Delete");
    testDeletingData();
    logger.log(Level.INFO, "Data Delete Test Completed");

    logger.log(Level.INFO, "Testing One to Many Fetch");
    testOneToManyData();
    logger.log(Level.INFO, "One To Many Fetch Test Completed");

    logger.log(Level.INFO, "Testing HQL List");
    testHqlList();
    logger.log(Level.INFO, "HQL List Test Completed");

    logger.log(Level.INFO, "Testing HQL Update");
    testHqlUpdate();
    logger.log(Level.INFO, "HQL Update Test Completed");

    logger.log(Level.INFO, "Testing JPA List and Update");
    testJPACriteria();
    logger.log(Level.INFO, "JPA List and Update Test Completed");

    logger.log(Level.INFO, "Testing JPA Delete");
    testJPACriteriaDelete();
    logger.log(Level.INFO, "JPA Delete Test Completed");
  }

  public static void main(String[] args) {
    HibernateSampleTest hibernateSampleTest =
        new HibernateSampleTest(HibernateConfiguration.createHibernateConfiguration());
    hibernateSampleTest.executeTest();
  }

}
