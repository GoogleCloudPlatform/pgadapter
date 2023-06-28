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
