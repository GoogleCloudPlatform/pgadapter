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

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.TableGenerator;
import java.io.Serializable;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

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

  @Id
  @GeneratedValue(strategy = GenerationType.TABLE, generator = "venue-generator")
  // Note that we reuse the 'seq-ids' table for different entities, but use a different name for
  // each entity. This ensures that there is a separate row in the table for each entity that uses
  // a table-backed sequence generator. This ensures that a single high-write entity does not cause
  // lock contention for all other entities that also use this type of identifier generation.
  @TableGenerator(
      name = "venue-generator",
      table = "seq_ids",
      pkColumnName = "seq_id",
      valueColumnName = "seq_value",
      initialValue = 1,
      // Use a sufficiently big allocation size to reduce the number of round-trips to the database
      // for generating new identifiers. An allocation size of 1000 means that we need a round-trip
      // to the database once for every 1000 records that we insert to generate identifiers.
      allocationSize = 1000)
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
