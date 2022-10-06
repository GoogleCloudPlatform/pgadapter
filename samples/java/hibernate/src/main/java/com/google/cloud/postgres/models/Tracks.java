package com.google.cloud.postgres.models;

import com.google.cloud.postgres.CurrentLocalDateTimeGenerator;
import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

@Entity
public class Tracks {

  @EmbeddedId
  private TracksId id;

  @Column(name = "title", nullable = false)
  private String title;

  @Column(name = "sample_rate")
  private double sampleRate;

  @Column(name = "created_at", columnDefinition = "timestamptz")
  private LocalDateTime createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Column(name = "updated_at", columnDefinition = "timestamptz")
  private LocalDateTime updatedAt;

  public TracksId getId() {
    return id;
  }

  public void setId(TracksId id) {
    this.id = id;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public double getSampleRate() {
    return sampleRate;
  }

  public void setSampleRate(double sampleRate) {
    this.sampleRate = sampleRate;
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
    return "Tracks{" +
        "id=" + id +
        ", title='" + title + '\'' +
        ", sampleRate=" + sampleRate +
        ", createdAt=" + createdAt +
        ", updatedAt=" + updatedAt +
        '}';
  }
}
