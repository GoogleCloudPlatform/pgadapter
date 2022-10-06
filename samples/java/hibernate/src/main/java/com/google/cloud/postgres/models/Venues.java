package com.google.cloud.postgres.models;

import com.google.cloud.postgres.CurrentLocalDateTimeGenerator;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

@Entity
public class Venues {

  @Id
  @Column(columnDefinition = "varchar", nullable = false)
  @GeneratedValue
  private UUID id;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "description", nullable = false)
  private String description;

  @Column(name = "created_at", columnDefinition = "timestamptz")
  private LocalDateTime createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Column(name = "updated_at", columnDefinition = "timestamptz")
  private LocalDateTime updatedAt;


  @OneToMany(cascade = CascadeType.ALL)
  @JoinColumn(name="venue_id")
  private List<Concerts> concerts;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
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

  public List<Concerts> getConcerts() {
    return concerts;
  }

  public void setConcerts(List<Concerts> concerts) {
    this.concerts = concerts;
  }

  @Override
  public String toString() {
    return "Venues{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", description='" + description + '\'' +
        ", createdAt=" + createdAt +
        ", updatedAt=" + updatedAt +
        ", concerts=" + concerts +
        '}';
  }
}
