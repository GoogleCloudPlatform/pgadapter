package com.google.cloud.myspan.entity;

import java.util.UUID;

public class MultiEntryIds {

  private TracksId tracksId;
  private UUID albumId;
  private UUID concertId;
  private UUID singersId;
  private UUID venueId;

  public MultiEntryIds(TracksId tracksId, UUID albumId, UUID concertId, UUID singersId,
      UUID venueId) {
    this.tracksId = tracksId;
    this.albumId = albumId;
    this.concertId = concertId;
    this.singersId = singersId;
    this.venueId = venueId;
  }

  public MultiEntryIds() {
  }

  public TracksId getTracksId() {
    return tracksId;
  }

  public void setTracksId(TracksId tracksId) {
    this.tracksId = tracksId;
  }

  public UUID getAlbumId() {
    return albumId;
  }

  public void setAlbumId(UUID albumId) {
    this.albumId = albumId;
  }

  public UUID getConcertId() {
    return concertId;
  }

  public void setConcertId(UUID concertId) {
    this.concertId = concertId;
  }

  public UUID getSingersId() {
    return singersId;
  }

  public void setSingersId(UUID singersId) {
    this.singersId = singersId;
  }

  public UUID getVenueId() {
    return venueId;
  }

  public void setVenueId(UUID venueId) {
    this.venueId = venueId;
  }
}
