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

import com.google.cloud.spanner.hibernate.EnhancedBitReversedSequenceStyleGenerator;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

@Table(name = "concerts")
@Entity
public class Concert extends AbstractBaseEntity {
  /**
   * This id is generated using a bit-reversed sequence in the database. The generator supports an
   * increment_size up to 60. This allows Hibernate to fetch up to 60 new identifiers in a single
   * round-trip to the database, and allows applications to enable JDBC batching in Hibernate.
   */
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "concert_id_generator")
  @GenericGenerator(
      // This is the name of the generator to use. This must correspond to the name in the
      // @GeneratedValue annotation above.
      name = "concert_id_generator",
      // This specifies the Cloud Spanner PostgreSQL bit-reversed sequence generator.
      type = EnhancedBitReversedSequenceStyleGenerator.class,
      parameters = {
        // This specifies the sequence name in the database.
        @Parameter(name = "sequence_name", value = "concert_id_sequence"),
        // Setting an increment size larger than 1 enables JDBC batching in Hibernate.
        // This generator supports increment_size between 1 and 60 (inclusive).
        @Parameter(name = "increment_size", value = "60"),
      })
  private long id;

  private String name;

  @ManyToOne(optional = false, fetch = FetchType.LAZY)
  private Venue venue;

  @ManyToOne(optional = false, fetch = FetchType.LAZY)
  private Singer singer;

  private OffsetDateTime startTime;

  private OffsetDateTime endTime;

  public Concert() {}

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Venue getVenue() {
    return venue;
  }

  public void setVenue(Venue venue) {
    this.venue = venue;
  }

  public Singer getSinger() {
    return singer;
  }

  public void setSinger(Singer singer) {
    this.singer = singer;
  }

  public OffsetDateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(OffsetDateTime startTime) {
    this.startTime = startTime;
  }

  public OffsetDateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(OffsetDateTime endTime) {
    this.endTime = endTime;
  }
}
