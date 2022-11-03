package com.google.cloud.postgres.models;

import com.google.cloud.postgres.CurrentLocalDateTimeGenerator;
import java.time.LocalDateTime;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

@Entity
public class Concerts {

  @Id
  @Column(columnDefinition = "varchar", nullable = false)
  @GeneratedValue
  private UUID id;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "start_time", columnDefinition = "timestamptz")
  private LocalDateTime startTime;

  @Column(name = "end_time", columnDefinition = "timestamptz")
  private LocalDateTime endTime;

  @ManyToOne
  @JoinColumn(name = "singer_id")
  private Singers singers;

  @ManyToOne
  @JoinColumn(name = "venue_id")
  private Venues venues;

  @Column(name = "created_at", columnDefinition = "timestamptz")
  private LocalDateTime createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Column(name = "updated_at", columnDefinition = "timestamptz")
  private LocalDateTime updatedAt;

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

  public LocalDateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(LocalDateTime startTime) {
    this.startTime = startTime;
  }

  public LocalDateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(LocalDateTime endTime) {
    this.endTime = endTime;
  }

  public Singers getSingers() {
    return singers;
  }

  public void setSingers(Singers singers) {
    this.singers = singers;
  }

  public Venues getVenues() {
    return venues;
  }

  public void setVenues(Venues venues) {
    this.venues = venues;
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
    return "Concerts{"
        + "id="
        + id
        + ", name='"
        + name
        + '\''
        + ", startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", singers="
        + singers
        + ", createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }
}
