// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample.model;

import com.google.cloud.spanner.pgadapter.sample.model.Track.TrackId;
import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.io.Serializable;
import java.util.Objects;
import org.springframework.data.domain.Persistable;

/**
 * Track extends AbstractBaseEntity that does not define any primary key. This allows us to define a
 * primary key in this entity, which again allows us to create a composite primary key. The latter
 * is required for interleaved tables.
 */
@Table(name = "tracks")
@Entity
public class Track extends AbstractBaseEntity implements Persistable<TrackId> {

  /**
   * Track is interleaved in the Album entity. This requires the primary key of Track to include all
   * the columns of the primary key of Album, in addition to its own primary key value. {@link
   * TrackId} defines the composite primary key of the {@link Track} entity.
   */
  @Embeddable
  public static class TrackId implements Serializable {
    /** `id` is the primary key column that Track 'inherits' from Album. */
    private String id;

    /** `trackNumber` is the additional primary key column that is used by Track. */
    private long trackNumber;

    protected TrackId() {}

    public TrackId(String id, long trackNumber) {
      this.id = id;
      this.trackNumber = trackNumber;
    }

    public String getId() {
      return id;
    }

    public long getTrackNumber() {
      return trackNumber;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, trackNumber);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TrackId)) {
        return false;
      }
      TrackId other = (TrackId) o;
      return Objects.equals(id, other.id) && Objects.equals(trackNumber, other.trackNumber);
    }
  }

  /** Factory method for creating a new {@link Track} belonging to an {@link Album}. */
  public static Track createNew(Album album, long trackNumber) {
    return new Track(album, trackNumber, true);
  }

  /** Hibernate requires a default constructor. */
  protected Track() {}

  private Track(Album album, long trackNumber, boolean newRecord) {
    setTrackId(new TrackId(album.getId(), trackNumber));
    this.newRecord = newRecord;
  }

  /** Use the @EmbeddedId annotation to define a composite primary key from an @Embeddable class. */
  @EmbeddedId private TrackId trackId;

  /** The "id" column is both part of the primary key, and a reference to the albums table. */
  @ManyToOne(optional = false)
  @JoinColumn(name = "id", updatable = false, insertable = false)
  private Album album;

  @Basic(optional = false)
  @Column(length = 100)
  private String title;

  private Double sampleRate;

  /**
   * This field is only used to track whether the entity has been persisted or not. This prevents
   * Hibernate from doing a round-trip to the database to check whether the Track exists every time
   * we call save(Track). The reason that we need this for this entity is that we manually assign
   * the primary key value to {@link Track}. That again means that Hibernate cannot determine
   * whether an instance of {@link Track} has already been persisted or not based on the existence
   * of a primary key value.
   */
  @Transient private boolean newRecord;

  @Override
  public TrackId getId() {
    return trackId;
  }

  @Override
  public boolean isNew() {
    return newRecord;
  }

  /** This method resets the 'newRecord' field after it has been persisted to the database. */
  @PostPersist
  public void resetPersisted() {
    newRecord = false;
  }

  public TrackId getTrackId() {
    return trackId;
  }

  public void setTrackId(TrackId trackId) {
    this.trackId = trackId;
  }

  public Album getAlbum() {
    return album;
  }

  public void setAlbum(Album album) {
    this.album = album;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Double getSampleRate() {
    return sampleRate;
  }

  public void setSampleRate(Double sampleRate) {
    this.sampleRate = sampleRate;
  }
}
