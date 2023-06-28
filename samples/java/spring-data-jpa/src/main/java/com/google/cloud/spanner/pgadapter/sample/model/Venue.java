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
  // a table-backed sequence generator.
  @TableGenerator(
      name = "venue-generator",
      table = "seq_ids",
      pkColumnName = "seq_id",
      valueColumnName = "seq_value",
      initialValue = 1,
      allocationSize = 1000)
  private long id;

  private String name;

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
