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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;
import org.hibernate.annotations.Type;

@Entity
public class Venues {

  @Id
  @Column(updatable = false, nullable = false, columnDefinition = "VARCHAR(255)")
  @GeneratedValue
  @Type(type = "uuid-char")
  private UUID id;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "description", nullable = false)
  private String description;

  @Column(name = "created_at")
  @Temporal(TemporalType.TIMESTAMP)
  private Date createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "updated_at")
  private Date updatedAt;

  @OneToMany(cascade = CascadeType.ALL)
  @JoinColumn(name = "venue_id")
  @JsonIgnore
  private List<Concerts> concerts;

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

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
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

  public List<Concerts> getConcerts() {
    return concerts;
  }

  public void setConcerts(List<Concerts> concerts) {
    this.concerts = concerts;
  }

  @Override
  public String toString() {
    return "Venues{"
        + "id="
        + id
        + ", name='"
        + name
        + '\''
        + ", description='"
        + description
        + '\''
        + ", createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }
}
