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
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.io.Serializable;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.Parameter;
import org.hibernate.type.SqlTypes;

/**
 * Venue uses a bit-reversed sequence to generate the primary key values. For this, it uses a
 * sequence generator specifically developed for Cloud Spanner PostgreSQL. The generator is the
 * {@link com.google.cloud.spanner.hibernate.EnhancedBitReversedSequenceStyleGenerator}.
 */
@Table(name = "venues")
@Entity
public class Venue extends AbstractBaseEntity {

  /**
   * {@link VenueDescription} is a POJO that is used for the JSONB field 'description' of the {@link
   * Venue} entity. It is automatically serialized and deserialized when an instance of the entity
   * is loaded or persisted.
   */
  public static class VenueDescription implements Serializable {
    private int capacity;
    private String type;
    private String location;

    public int getCapacity() {
      return capacity;
    }

    public void setCapacity(int capacity) {
      this.capacity = capacity;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }
  }

  /**
   * This id is generated using a bit-reversed sequence in the database. The generator supports an
   * increment_size up to 60. This allows Hibernate to fetch up to 60 new identifiers in a single
   * round-trip to the database, and allows applications to enable JDBC batching in Hibernate.
   */
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "venue_id_generator")
  @GenericGenerator(
      // This is the name of the generator to use. This must correspond to the name in the
      // @GeneratedValue annotation above.
      name = "venue_id_generator",
      // This specifies the Cloud Spanner bit-reversed sequence generator.
      type = EnhancedBitReversedSequenceStyleGenerator.class,
      parameters = {
        // This specifies the sequence name in the database.
        @Parameter(name = "sequence_name", value = "venue_id_sequence"),
        // Setting an increment size larger than 1 enables JDBC batching in Hibernate.
        // This generator supports increment_size between 1 and 60 (inclusive).
        @Parameter(name = "increment_size", value = "60"),
        // The initial counter value of the bit-reversed sequence.
        @Parameter(name = "initial_value", value = "5000"),
        // Specify a range that should be excluded from generation by the sequence if you already
        // have rows in your table with identifier values in a specific range.
        @Parameter(name = "exclude_range", value = "[10000,20000]")
      })
  private long id;

  private String name;

  /**
   * This field maps to a JSONB column in the database. The value is automatically
   * serialized/deserialized to a {@link VenueDescription} instance.
   */
  @JdbcTypeCode(SqlTypes.JSON)
  private VenueDescription description;

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

  public VenueDescription getDescription() {
    return description;
  }

  public void setDescription(VenueDescription description) {
    this.description = description;
  }
}
