package com.google.cloud.postgres;

import com.google.cloud.postgres.models.Albums;
import com.google.cloud.postgres.models.Concerts;
import com.google.cloud.postgres.models.Singers;
import com.google.cloud.postgres.models.Tracks;
import com.google.cloud.postgres.models.TracksId;
import com.google.cloud.postgres.models.Venues;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

public class Utils {

  private static Random random = new Random();

  public static Singers createSingers() {
    final Singers singers = new Singers();
    singers.setActive(true);
    singers.setFirstName("David");
    singers.setLastName("Lee");
    singers.setCreatedAt(LocalDateTime.now());
    return singers;
  }

  public static Albums createAlbums(Singers singers) {
    final Albums albums = new Albums();
    albums.setTitle("Perfect");
    albums.setMarketingBudget(new BigDecimal("1.00"));
    albums.setReleaseDate(LocalDate.now());
    albums.setCreatedAt(LocalDateTime.now());
    albums.setSingers(singers);
    return albums;
  }

  public static Concerts createConcerts(Singers singers, Venues venues) {
    final Concerts concerts = new Concerts();
    concerts.setCreatedAt(LocalDateTime.now());
    concerts.setEndTime(LocalDateTime.now().plusHours(1));
    concerts.setStartTime(LocalDateTime.now());
    concerts.setName("Sunburn");
    concerts.setSingers(singers);
    concerts.setVenues(venues);
    return concerts;
  }

  public static Tracks createTracks(UUID albumId) {
    final Tracks tracks = new Tracks();
    tracks.setCreatedAt(LocalDateTime.now());
    tracks.setTitle("Perfect");
    tracks.setSampleRate(random.nextInt());
    TracksId tracksId = new TracksId();
    tracksId.setTrackNumber(random.nextInt());
    tracksId.setId(albumId);
    tracks.setId(tracksId);
    return tracks;
  }

  public static Venues createVenue() {
    final Venues venues = new Venues();
    venues.setCreatedAt(LocalDateTime.now());
    venues.setName("Hall");
    venues.setDescription("Theater");

    return venues;
  }
}
