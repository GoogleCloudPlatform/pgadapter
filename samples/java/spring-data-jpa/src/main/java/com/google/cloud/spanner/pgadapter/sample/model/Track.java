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

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.util.Objects;

/** Track extends AbstractBaseEntity that does not define any primary key. This allows us to define a primary key in this entity, which again allows us to create a composite primary key. The latter is required for interleaved tables. */
@Table(name = "tracks")
@Entity
public class Track extends AbstractBaseEntity {

  @Embeddable
  public static class TrackId implements Serializable {
    private String id;

    private long trackNumber;
    
    protected TrackId() {}

    public TrackId(String id, int trackNumber) {
      this.id = id;
      this.trackNumber = trackNumber;
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(id, trackNumber);
    }
    
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TrackId other)) {
        return false;
      }
      return Objects.equals(id, other.id) && Objects.equals(trackNumber, other.trackNumber);
    }
  }
  
  @EmbeddedId
  private TrackId trackId;
  
  @Basic(optional = false)
  @Column(length = 100)
  private String title;
  
  private Double sampleRate;

  public TrackId getTrackId() {
    return trackId;
  }

  public void setTrackId(TrackId trackId) {
    this.trackId = trackId;
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
