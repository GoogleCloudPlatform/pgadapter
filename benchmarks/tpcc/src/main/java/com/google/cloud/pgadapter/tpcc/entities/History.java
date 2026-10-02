// Copyright 2024 Google LLC
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
package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;

@Entity
@Table(name = "history")
public class History {

  @EmbeddedId private HistoryId id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumns({
    @JoinColumn(
        name = "w_id",
        referencedColumnName = "w_id",
        insertable = false,
        updatable = false),
    @JoinColumn(
        name = "d_id",
        referencedColumnName = "d_id",
        insertable = false,
        updatable = false),
    @JoinColumn(name = "c_id", referencedColumnName = "c_id", insertable = false, updatable = false)
  })
  private Customer customer;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumns({
    @JoinColumn(
        name = "h_w_id",
        referencedColumnName = "w_id",
        insertable = false,
        updatable = false),
    @JoinColumn(
        name = "h_d_id",
        referencedColumnName = "d_id",
        insertable = false,
        updatable = false)
  })
  private District district;

  @Column(name = "h_amount")
  private BigDecimal amount;

  @Column(name = "h_data")
  private String data;

  public HistoryId getId() {
    return id;
  }

  public void setId(HistoryId id) {
    this.id = id;
  }

  public Customer getCustomer() {
    return customer;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  public District getDistrict() {
    return district;
  }

  public void setDistrict(District district) {
    this.district = district;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }
}
