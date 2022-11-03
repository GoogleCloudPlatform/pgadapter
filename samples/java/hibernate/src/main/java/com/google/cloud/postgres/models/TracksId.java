package com.google.cloud.postgres.models;

import java.io.Serializable;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class TracksId implements Serializable {

  @Column(columnDefinition = "varchar", nullable = false)
  private UUID id;

  @Column(name = "track_number", nullable = false)
  private long trackNumber;

  public TracksId() {}

  public TracksId(UUID id, long trackNumber) {
    this.id = id;
    this.trackNumber = trackNumber;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public long getTrackNumber() {
    return trackNumber;
  }

  public void setTrackNumber(long trackNumber) {
    this.trackNumber = trackNumber;
  }

  @Override
  public String toString() {
    return "TracksId{" + "id=" + id + ", trackNumber=" + trackNumber + '}';
  }
}
