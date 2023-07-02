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

import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;

/**
 * Abstract base class for entities that use a UUID as the primary key.
 *
 * <p>UUID is the recommended primary key type for Cloud Spanner, as it does not require any
 * additional round-trips to the database to generate a primary key value.
 *
 * <p>See {@link Venue} for an example of how to set up an entity that uses a sequential integer
 * value as a primary key.
 */
@MappedSuperclass
public class AbstractUuidEntity extends AbstractBaseEntity {

  /** Use a UUID as primary key. */
  @Id
  @Column(columnDefinition = "varchar(36)")
  @GeneratedValue(strategy = GenerationType.UUID)
  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
