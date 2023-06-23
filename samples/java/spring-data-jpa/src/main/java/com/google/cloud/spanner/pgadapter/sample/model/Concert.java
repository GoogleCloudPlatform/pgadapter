package com.google.cloud.spanner.pgadapter.sample.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.TableGenerator;

@Table(name = "concerts")
@Entity
public class Concert extends AbstractBaseEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.TABLE, generator = "concert-generator")
  @TableGenerator(name = "concert-generator",
      table = "seq_ids",
      pkColumnName = "seq_id",
      valueColumnName = "seq_value",
      initialValue = 1, allocationSize = 1000)
  private long id;
}
