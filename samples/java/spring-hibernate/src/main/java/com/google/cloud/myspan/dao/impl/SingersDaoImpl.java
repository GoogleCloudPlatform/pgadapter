package com.google.cloud.myspan.dao.impl;

import com.google.cloud.myspan.controller.DemoController;
import com.google.cloud.myspan.entity.Albums;
import com.google.cloud.myspan.entity.Concerts;
import com.google.cloud.myspan.entity.Singers;
import com.google.cloud.myspan.entity.Tracks;
import com.google.cloud.myspan.entity.TracksId;
import com.google.cloud.myspan.entity.Venues;
import com.google.cloud.myspan.dao.SingersDao;
import java.util.List;
import java.util.UUID;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Gulam Mustafa
 */
@Component
public class SingersDaoImpl implements SingersDao {
  private static final Logger logger = LoggerFactory.getLogger(DemoController.class);

  @Autowired
  private SessionFactory sessionFactory;

  @Override
  public List<Singers> getAllSingers() {
    Criteria criteria = sessionFactory.openSession().createCriteria(Singers.class);
    return criteria.list();
  }

  @Override
  public Concerts getConcert(UUID uuid) {
    return sessionFactory.openSession().get(Concerts.class, uuid);
  }

  @Override
  public Albums getAlbum(UUID uuid) {
    return sessionFactory.openSession().get(Albums.class, uuid);
  }

  @Override
  public Singers getSingers(UUID uuid) {
    return sessionFactory.openSession().get(Singers.class, uuid);
  }

  @Override
  public List<Tracks> getTracks(UUID albumId) {
    Criteria cb = sessionFactory.openSession().createCriteria(Tracks.class);
    cb.add(Restrictions.eq("id.id", albumId));
    return cb.list();
  }

  @Override
  public int updateAlbumMarketingBudget(Albums albums) {
    Session session = sessionFactory.openSession();
    session.beginTransaction();
    session.update(albums);
    session.getTransaction().commit();
    session.flush();
    return 1;
  }

  @Override
  public UUID insertSinger(Singers singers) {
    Session session = sessionFactory.openSession();
    session.saveOrUpdate(singers);
    session.flush();
    return singers.getId();
  }

  @Override
  public TracksId multiObjectsInTransaction(Singers singers, Albums albums, Venues venues,
      Concerts concerts,
      Tracks tracks) {

    Session session = sessionFactory.openSession();
    session.beginTransaction();
    session.saveOrUpdate(singers);
    session.saveOrUpdate(albums);
    session.saveOrUpdate(venues);
    session.saveOrUpdate(concerts);
    tracks.getId().setId(albums.getId());
    session.saveOrUpdate(tracks);
    session.getTransaction().commit();
    return tracks.getId();
  }

  @Override
  public UUID insertAlbum(Albums albums) {
    Session session = sessionFactory.openSession();
    session.beginTransaction();
    session.saveOrUpdate(albums);
    session.flush();
    return albums.getId();
  }

  @Override
  public UUID insertConcerts(Concerts concerts) {
    Session session = sessionFactory.openSession();
    session.beginTransaction();
    session.saveOrUpdate(concerts);
    session.flush();
    return concerts.getId();
  }

  @Override
  public TracksId insertTracks(Tracks tracks) {
    Session session = sessionFactory.openSession();
    session.beginTransaction();
    session.saveOrUpdate(tracks);
    session.flush();
    return tracks.getId();
  }

  @Override
  public UUID rollbackTest(Singers singers) {
    Session session = sessionFactory.openSession();
    session.beginTransaction();
    session.saveOrUpdate(singers);
    logger.info("Added singers: {}", singers);
    session.flush();
    session.getTransaction().rollback();
    logger.info("Added row has been rolled back.");
    return singers.getId();
  }

}
