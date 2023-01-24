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
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;
import org.hibernate.annotations.Type;

@Entity
public class Concerts {

  @Id
  @Column(updatable = false, nullable = false, columnDefinition = "VARCHAR(255)")
  @GeneratedValue
  @Type(type = "uuid-char")
  private UUID id;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "start_time")
  private Date startTime;

  @Column(name = "end_time")
  private Date endTime;

  @ManyToOne
  @JoinColumn(name = "singer_id")
  private Singers singers;

  @ManyToOne
  @JoinColumn(name = "venue_id")
  private Venues venues;

  @Column(name = "created_at")
  @Temporal(TemporalType.TIMESTAMP)
  private Date createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Column(name = "updated_at")
  @Temporal(TemporalType.TIMESTAMP)
  private Date updatedAt;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  public Singers getSingers() {
    return singers;
  }

  public void setSingers(Singers singers) {
    this.singers = singers;
  }

  public Venues getVenues() {
    return venues;
  }

  public void setVenues(Venues venues) {
    this.venues = venues;
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
    return "Concerts{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", startTime=" + startTime +
        ", endTime=" + endTime +
        ", singers=" + singers +
        ", venues=" + venues +
        ", createdAt=" + createdAt +
        ", updatedAt=" + updatedAt +
        '}';
  }
}
