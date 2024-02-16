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

import com.google.cloud.postgres.CurrentLocalDateTimeGenerator;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import java.time.LocalDateTime;
import java.util.List;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

@Entity
public class Concerts {

  @Id
  @Column(columnDefinition = "varchar(36)")
  @GeneratedValue(strategy = GenerationType.UUID)
  private String id;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "start_time", columnDefinition = "timestamptz")
  private LocalDateTime startTime;

  @Column(name = "end_time", columnDefinition = "timestamptz")
  private LocalDateTime endTime;

  @ManyToOne
  @JoinColumn(name = "singer_id")
  private Singers singers;

  @ManyToOne
  @JoinColumn(name = "venue_id")
  private Venues venues;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "concert")
  private List<TicketSale> ticketSales;

  @Column(name = "created_at", columnDefinition = "timestamptz")
  private LocalDateTime createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Column(name = "updated_at", columnDefinition = "timestamptz")
  private LocalDateTime updatedAt;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public LocalDateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(LocalDateTime startTime) {
    this.startTime = startTime;
  }

  public LocalDateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(LocalDateTime endTime) {
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

  public List<TicketSale> getTicketSales() {
    return ticketSales;
  }

  public void setTicketSales(List<TicketSale> ticketSales) {
    this.ticketSales = ticketSales;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public LocalDateTime getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(LocalDateTime updatedAt) {
    this.updatedAt = updatedAt;
  }

  @Override
  public String toString() {
    return "Concerts{"
        + "id="
        + id
        + ", name='"
        + name
        + '\''
        + ", startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", singers="
        + singers
        + ", createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }
}
