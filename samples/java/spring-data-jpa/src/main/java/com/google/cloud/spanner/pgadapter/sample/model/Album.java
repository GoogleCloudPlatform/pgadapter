// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.sample.model;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.LocalDate;

@Table(name = "albums")
@Entity
public class Album extends AbstractUuidEntity {
  
  @Basic(optional = false)
  @Column(length = 200)
  private String title;

  private BigDecimal marketingBudget;

  private LocalDate releaseDate;
  
  private byte[] coverPicture;
  
  @ManyToOne(optional = false)
  private Singer singer;

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

  public LocalDate getReleaseDate() {
    return releaseDate;
  }

  public void setReleaseDate(LocalDate releaseDate) {
    this.releaseDate = releaseDate;
  }

  public byte[] getCoverPicture() {
    return coverPicture;
  }

  public void setCoverPicture(byte[] coverPicture) {
    this.coverPicture = coverPicture;
  }

  public Singer getSinger() {
    return singer;
  }

  public void setSinger(Singer singer) {
    this.singer = singer;
  }
}
