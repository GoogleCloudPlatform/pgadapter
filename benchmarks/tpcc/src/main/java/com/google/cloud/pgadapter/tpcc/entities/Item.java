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
@Table(name = "item")
public class Item {

  @Id
  @Column(name = "i_id")
  private Long iId;

  @Column(name = "i_im_id")
  private Long iImId;

  @Column(name = "i_name", length = 24)
  private String iName;

  @Column(name = "i_price", precision = 12, scale = 4)
  private BigDecimal iPrice;

  @Column(name = "i_data", length = 50)
  private String iData;

  public Long getiId() {
    return iId;
  }

  public void setiId(Long iId) {
    this.iId = iId;
  }

  public Long getiImId() {
    return iImId;
  }

  public void setiImId(Long iImId) {
    this.iImId = iImId;
  }

  public String getiName() {
    return iName;
  }

  public void setiName(String iName) {
    this.iName = iName;
  }

  public BigDecimal getiPrice() {
    return iPrice;
  }

  public void setiPrice(BigDecimal iPrice) {
    this.iPrice = iPrice;
  }

  public String getiData() {
    return iData;
  }

  public void setiData(String iData) {
    this.iData = iData;
  }
}
