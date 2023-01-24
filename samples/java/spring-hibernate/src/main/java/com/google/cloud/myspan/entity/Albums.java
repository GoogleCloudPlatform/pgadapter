package com.google.cloud.myspan.entity;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;
import org.hibernate.annotations.Type;

@Entity
public class Albums {

  @Id
  @Column(updatable = false, nullable = false, columnDefinition = "VARCHAR(255)")
  @GeneratedValue
  @Type(type = "uuid-char")
  private UUID id;

  private String title;

  @Column(name = "marketing_budget")
  private BigDecimal marketingBudget;

  @Column(name = "release_date")
  private Date releaseDate;

  @ManyToOne
  @JoinColumn(name = "singer_id")
  private Singers singers;

  @Column(name = "created_at")
  @Temporal(TemporalType.TIMESTAMP)
  private Date createdAt;

  @GeneratorType(type = CurrentLocalDateTimeGenerator.class, when = GenerationTime.ALWAYS)
  @Column(name = "updated_at")
  @Temporal(TemporalType.TIMESTAMP)
  private Date updatedAt;

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

  public Date getReleaseDate() {
    return releaseDate;
  }

  public void setReleaseDate(Date releaseDate) {
    this.releaseDate = releaseDate;
  }

  public Singers getSingers() {
    return singers;
  }

  public void setSingers(Singers singers) {
    this.singers = singers;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Date updatedAt) {
    this.updatedAt = updatedAt;
  }

  @Override
  public String toString() {
    return "Albums{" +
        "id=" + id +
        ", title='" + title + '\'' +
        ", marketingBudget=" + marketingBudget +
        ", releaseDate=" + releaseDate +
        ", singers=" + singers +
        ", createdAt=" + createdAt +
        ", updatedAt=" + updatedAt +
        '}';
  }
}
