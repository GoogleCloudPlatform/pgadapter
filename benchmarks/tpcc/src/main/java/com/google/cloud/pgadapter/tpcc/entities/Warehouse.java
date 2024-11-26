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
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;

@Entity
@Table(name = "warehouse")
public class Warehouse {

  @Id
  @Column(name = "w_id")
  private Long wId;

  @Column(name = "w_name", length = 10)
  private String wName;

  @Column(name = "w_street_1", length = 20)
  private String wStreet1;

  @Column(name = "w_street_2", length = 20)
  private String wStreet2;

  @Column(name = "w_city", length = 20)
  private String wCity;

  @Column(name = "w_state", length = 2)
  private String wState;

  @Column(name = "w_zip", length = 9)
  private String wZip;

  @Column(name = "w_tax", precision = 12, scale = 4)
  private BigDecimal wTax;

  @Column(name = "w_ytd", precision = 12, scale = 4)
  private BigDecimal wYtd;

  public Long getwId() {
    return wId;
  }

  public void setwId(Long wId) {
    this.wId = wId;
  }

  public String getwName() {
    return wName;
  }

  public void setwName(String wName) {
    this.wName = wName;
  }

  public String getwStreet1() {
    return wStreet1;
  }

  public void setwStreet1(String wStreet1) {
    this.wStreet1 = wStreet1;
  }

  public String getwStreet2() {
    return wStreet2;
  }

  public void setwStreet2(String wStreet2) {
    this.wStreet2 = wStreet2;
  }

  public String getwCity() {
    return wCity;
  }

  public void setwCity(String wCity) {
    this.wCity = wCity;
  }

  public String getwState() {
    return wState;
  }

  public void setwState(String wState) {
    this.wState = wState;
  }

  public String getwZip() {
    return wZip;
  }

  public void setwZip(String wZip) {
    this.wZip = wZip;
  }

  public BigDecimal getwTax() {
    return wTax;
  }

  public void setwTax(BigDecimal wTax) {
    this.wTax = wTax;
  }

  public BigDecimal getwYtd() {
    return wYtd;
  }

  public void setwYtd(BigDecimal wYtd) {
    this.wYtd = wYtd;
  }
}
