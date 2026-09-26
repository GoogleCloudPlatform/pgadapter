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
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import org.hibernate.annotations.DynamicUpdate;

@Entity
@DynamicUpdate
@Table(name = "district")
public class District {

  @EmbeddedId private DistrictId id;

  @ManyToOne(fetch = FetchType.EAGER)
  @JoinColumn(name = "w_id", referencedColumnName = "w_id", insertable = false, updatable = false)
  private Warehouse warehouse;

  @Column(name = "d_name", length = 10)
  private String dName;

  @Column(name = "d_street_1", length = 20)
  private String dStreet1;

  @Column(name = "d_street_2", length = 20)
  private String dStreet2;

  @Column(name = "d_city", length = 20)
  private String dCity;

  @Column(name = "d_state", length = 2)
  private String dState;

  @Column(name = "d_zip", length = 9)
  private String dZip;

  @Column(name = "d_tax", precision = 12, scale = 4)
  private BigDecimal dTax;

  @Column(name = "d_ytd", precision = 12, scale = 4)
  private BigDecimal dYtd;

  @Column(name = "d_next_o_id")
  private Long dNextOId;

  public DistrictId getId() {
    return id;
  }

  public void setId(DistrictId id) {
    this.id = id;
  }

  public Warehouse getWarehouse() {
    return warehouse;
  }

  public void setWarehouse(Warehouse warehouse) {
    this.warehouse = warehouse;
  }

  public String getdName() {
    return dName;
  }

  public void setdName(String dName) {
    this.dName = dName;
  }

  public String getdStreet1() {
    return dStreet1;
  }

  public void setdStreet1(String dStreet1) {
    this.dStreet1 = dStreet1;
  }

  public String getdStreet2() {
    return dStreet2;
  }

  public void setdStreet2(String dStreet2) {
    this.dStreet2 = dStreet2;
  }

  public String getdCity() {
    return dCity;
  }

  public void setdCity(String dCity) {
    this.dCity = dCity;
  }

  public String getdState() {
    return dState;
  }

  public void setdState(String dState) {
    this.dState = dState;
  }

  public String getdZip() {
    return dZip;
  }

  public void setdZip(String dZip) {
    this.dZip = dZip;
  }

  public BigDecimal getdTax() {
    return dTax;
  }

  public void setdTax(BigDecimal dTax) {
    this.dTax = dTax;
  }

  public BigDecimal getdYtd() {
    return dYtd;
  }

  public void setdYtd(BigDecimal dYtd) {
    this.dYtd = dYtd;
  }

  public Long getdNextOId() {
    return dNextOId;
  }

  public void setdNextOId(Long dNextOId) {
    this.dNextOId = dNextOId;
  }
}
