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

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

@Entity
public class Tracks {

  // For composite primary keys, @EmbeddedId will have to be used.
  @EmbeddedId
  private TracksId id;

  @Column(name = "title", nullable = false)
  private String title;

  @Column(name = "sample_rate")
  private double sampleRate;

  @Column(name = "created_at")
  @Temporal(TemporalType.TIMESTAMP)
  private Date createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "updated_at")
  private Date updatedAt;

  public TracksId getId() {
    return id;
  }

  public void setId(TracksId id) {
    this.id = id;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public double getSampleRate() {
    return sampleRate;
  }

  public void setSampleRate(double sampleRate) {
    this.sampleRate = sampleRate;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Date updatedAt) {
    this.updatedAt = updatedAt;
  }

  @Override
  public String toString() {
    return "Tracks{"
        + "id="
        + id
        + ", title='"
        + title
        + '\''
        + ", sampleRate="
        + sampleRate
        + ", createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }
}
