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

package com.google.cloud.postgres.models;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;

/** {@link Embeddable} is to be used for composite primary key. */
@Embeddable
public class TracksId implements Serializable {

  @Column(columnDefinition = "varchar(36)")
  private String id;

  @Column(name = "track_number", nullable = false)
  private long trackNumber;

  public TracksId() {}

  public TracksId(String id, long trackNumber) {
    this.id = id;
    this.trackNumber = trackNumber;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
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
