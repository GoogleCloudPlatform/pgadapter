package com.google.cloud.postgres.models;

import com.google.cloud.postgres.CurrentLocalDateTimeGenerator;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;
import org.hibernate.annotations.Type;

@Entity
public class Albums {

  @Id
  @Column(columnDefinition = "varchar", nullable = false)
  @GeneratedValue
  private UUID id;

  private String title;

  @Column(name = "marketing_budget")
  private BigDecimal marketingBudget;

  @Column(name = "release_date", columnDefinition = "date")
  private LocalDate releaseDate;

  @Lob
  @Type(type = "org.hibernate.type.BinaryType")
  @Column(name = "cover_picture")
  private byte[] coverPicture;

  @ManyToOne
  @JoinColumn(name = "singer_id")
  private Singers singers;

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

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public BigDecimal getMarketingBudget() {
    return marketingBudget;
  }

  public void setMarketingBudget(BigDecimal marketingBudget) {
    this.marketingBudget = marketingBudget;
  }

  public byte[] getCoverPicture() {
    return coverPicture;
  }

  public void setCoverPicture(byte[] coverPicture) {
    this.coverPicture = coverPicture;
  }

  public Singers getSingers() {
    return singers;
  }

  public void setSingers(Singers singers) {
    this.singers = singers;
  }

  public LocalDate getReleaseDate() {
    return releaseDate;
  }

  public void setReleaseDate(LocalDate releaseDate) {
    this.releaseDate = releaseDate;
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
    return "Albums{"
        + "id='"
        + id
        + '\''
        + ", title='"
        + title
        + '\''
        + ", marketingBudget="
        + marketingBudget
        + ", releaseDate="
        + releaseDate
        + ", coverPicture="
        + Arrays.toString(coverPicture)
        + ", singers="
        + singers
        + ", createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }
}
