package com.google.cloud.postgres.models;

import com.google.cloud.postgres.CustomValueGenerator;
import java.time.LocalDateTime;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

@Entity
public class Singers {

  @Id
  @Column(columnDefinition = "varchar", nullable = false)
  @GeneratedValue
  private UUID id;

  @Column(name = "first_name")
  private String firstName;

  @Column(name = "last_name", nullable = false)
  private String lastName;

  @Column(name = "full_name", insertable = false)
  @Generated(GenerationTime.ALWAYS)
  private String fullName;

  private boolean active;

  @Column(name = "created_at", columnDefinition = "timestamptz")
  private LocalDateTime createdAt;

  @GeneratorType(type = CustomValueGenerator.class, when = GenerationTime.ALWAYS)
  @Column(name = "updated_at", columnDefinition = "timestamptz")
  private LocalDateTime updatedAt;


  public UUID getId() {
    return id;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getFullName() {
    return fullName;
  }

  public boolean isActive() {
    return active;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public void setActive(boolean active) {
    this.active = active;
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
    return "Singers{" +
        "id='" + id + '\'' +
        ", firstName='" + firstName + '\'' +
        ", lastName='" + lastName + '\'' +
        ", fullName='" + fullName + '\'' +
        ", active=" + active +
        ", createdAt=" + createdAt +
        ", updatedAt=" + updatedAt +
        '}';
  }
}
