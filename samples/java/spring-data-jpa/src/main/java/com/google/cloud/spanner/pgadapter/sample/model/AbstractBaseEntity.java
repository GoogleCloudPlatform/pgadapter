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
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * Base class for all entities that are used by this sample application.
 * <p>
 * This class defines the createdAt and updatedAt properties that are present in all entities.
 */
@MappedSuperclass
public class AbstractBaseEntity {

  /** PostgreSQL by default maps ZonedDateTime to 'timestamptz(6). Type modifiers are not supported for timestamptz in Cloud Spanner, so we override the default. */
  @Column(columnDefinition = "timestamptz")
  @CreationTimestamp
  private ZonedDateTime createdAt;
  
  /** PostgreSQL by default maps ZonedDateTime to 'timestamptz(6). Type modifiers are not supported for timestamptz in Cloud Spanner, so we override the default. */
  @Column(columnDefinition = "timestamptz")
  @UpdateTimestamp
  private ZonedDateTime updatedAt;
  
}
