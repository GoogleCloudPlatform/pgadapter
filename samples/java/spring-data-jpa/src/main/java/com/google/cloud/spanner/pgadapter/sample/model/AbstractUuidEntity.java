package com.google.cloud.spanner.pgadapter.sample.model;

import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;

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
