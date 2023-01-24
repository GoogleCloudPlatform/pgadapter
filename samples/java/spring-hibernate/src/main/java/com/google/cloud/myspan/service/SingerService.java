/**
 *
 */
package com.google.cloud.myspan.service;

import com.google.cloud.myspan.controller.DemoController;
import com.google.cloud.myspan.dao.SingersDao;
import com.google.cloud.myspan.entity.Albums;
import com.google.cloud.myspan.entity.Concerts;
import com.google.cloud.myspan.entity.MultiEntryIds;
import com.google.cloud.myspan.entity.Singers;
import com.google.cloud.myspan.entity.Tracks;
import com.google.cloud.myspan.entity.TracksId;
import com.google.cloud.myspan.entity.Utils;
import com.google.cloud.myspan.entity.Venues;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author Gulam Mustafa
 */
@Service
public class SingerService {

  private static final Logger logger = LoggerFactory.getLogger(DemoController.class);

  @Autowired
  private SingersDao singersDao;

  public List<Singers> getSingers() {
    logger.info("Fetching all singers!");
    List<Singers> singers = singersDao.getAllSingers();

    logger.info("Fetched Singers: {}", singers);
    return singers;
  }

  public Concerts getConcerts(UUID uuid) {

    logger.info("Fetching concerts. This joins Concerts, Venues and Singers tables.");
    Concerts concerts = singersDao.getConcert(uuid);
    logger.info("Fetched Concerts: {}", concerts);

    return concerts;
  }

  public Albums getAlbum(UUID albumId) {

    logger.info("Fetching Albums. This joins Albums and Singers tables.");
    Albums albums = singersDao.getAlbum(albumId);
    logger.info("Fetched Albums: {}", albums);
    return albums;
  }

  public Singers getSingers(UUID singers) {
    return singersDao.getSingers(singers);
  }

  public Albums updateAlbumMarketingBudget(UUID albumId, String marketingBudget) {
    logger.info("Album to be updated.");
    Albums albums = singersDao.getAlbum(albumId);
    albums.setMarketingBudget(new BigDecimal(marketingBudget));
    singersDao.updateAlbumMarketingBudget(albums);
    logger.info("Updated album: {}", albums);
    return albums;
  }

  public List<Tracks> getTracksForAlbums(UUID albumId) {
    return singersDao.getTracks(albumId);
  }

  public UUID addSinger(String firstName, String lastName) {
    return singersDao.insertSinger(new Singers(firstName, lastName));
  }

  public UUID rollback() {
    logger.info("Performing rollback test.");
    return singersDao.rollbackTest(new Singers("Ed", "Sheeran"));
  }

  public MultiEntryIds addTracksWithSingers(String trackName, int trackNumber, int sampleRate,
      String concertName, String venueName, String venueDescription, String albumName,
      String marketingBudget, String singerFirstName, String singerLastName) {

    logger.info("Creating singers, albums, venues, concerts and tracks in single transaction.");

    final Singers singers = Utils.createSingers(singerFirstName, singerLastName);
    final Albums albums = Utils.createAlbums(singers, albumName, marketingBudget);
    final Venues venues = Utils.createVenue(venueName, venueDescription);
    final Concerts concerts1 = Utils.createConcerts(singers, venues, concertName);
    final Tracks tracks1 = Utils.createTracks(albums.getId(), trackName, trackNumber, sampleRate);
    singersDao.multiObjectsInTransaction(singers, albums, venues, concerts1, tracks1);
    MultiEntryIds multiEntryIds = new MultiEntryIds(tracks1.getId(), albums.getId(),
        concerts1.getId(), singers.getId(),
        venues.getId());

    logger.info("Singers created: {}", singers);
    logger.info("Albums created: {}", albums);
    logger.info("Venues created: {}", venues);
    logger.info("Concerts created: {}", concerts1);
    logger.info("Tracks created: {}", tracks1);

    return multiEntryIds;
  }


}
