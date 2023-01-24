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

package com.google.cloud.myspan.entity;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class Utils {

  private static Random random = new Random();

  public static Singers createSingers() {
    final Singers singers = new Singers();
    // singers.setActive(true);
    singers.setFirstName("David");
    singers.setLastName("Lee");
    singers.setCreatedAt(new Date());
    return singers;
  }

  public static Singers createSingers(String singerFirstName, String singerLastName) {
    final Singers singers = new Singers();
    singers.setFirstName(singerFirstName);
    singers.setLastName(singerLastName);
    singers.setCreatedAt(new Date());
    return singers;
  }

  public static Albums createAlbums(Singers singers, String name, String budget) {
    final Albums albums = new Albums();
    albums.setTitle(name);
    albums.setMarketingBudget(new BigDecimal(budget));
    albums.setReleaseDate(new Date());
    albums.setCreatedAt(new Date());
    albums.setSingers(singers);
    return albums;
  }

  public static Concerts createConcerts(Singers singers, Venues venues, String name) {
    final Concerts concerts = new Concerts();
    concerts.setCreatedAt(new Date());
    concerts.setEndTime(new Date());
    concerts.setStartTime(new Date());
    concerts.setName(name);
    concerts.setSingers(singers);
    concerts.setVenues(venues);
    return concerts;
  }

  public static Tracks createTracks(UUID albumId, String trackName) {
    final Tracks tracks = new Tracks();
    tracks.setCreatedAt(new Date());
    tracks.setTitle(trackName);
    tracks.setSampleRate(random.nextInt());
    TracksId tracksId = new TracksId();
    tracksId.setTrackNumber(random.nextInt());
    tracksId.setId(albumId);
    tracks.setId(tracksId);
    return tracks;
  }

  public static Tracks createTracks(UUID albumId, String trackName, int sampleRate,
      int trackNumber) {
    final Tracks tracks = new Tracks();
    tracks.setCreatedAt(new Date());
    tracks.setTitle(trackName);
    tracks.setSampleRate(sampleRate);
    TracksId tracksId = new TracksId();
    tracksId.setTrackNumber(trackNumber);
    tracksId.setId(albumId);
    tracks.setId(tracksId);
    return tracks;
  }

  public static Venues createVenue(String venueName, String desc) {
    final Venues venues = new Venues();
    venues.setCreatedAt(new Date());
    venues.setName(venueName);
    venues.setDescription(desc);

    return venues;
  }
}
