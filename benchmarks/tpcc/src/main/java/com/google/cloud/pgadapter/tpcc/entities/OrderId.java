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

import com.google.common.base.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
public class OrderId implements Serializable {

  @Column(name = "o_id")
  private Long oId;

  @Column(name = "c_id")
  private Long cId;

  @Column(name = "d_id")
  private Long dId;

  @Column(name = "w_id")
  private Long wId;

  public OrderId() {
  }

  public OrderId(long newOrderId, long customerId, long districtId, long warehouseId) {
    this.oId = newOrderId;
    this.cId = customerId;
    this.dId = districtId;
    this.wId = warehouseId;
  }

  public Long getoId() {
    return oId;
  }

  public void setoId(Long oId) {
    this.oId = oId;
  }

  public Long getcId() {
    return cId;
  }

  public void setcId(Long cId) {
    this.cId = cId;
  }

  public Long getdId() {
    return dId;
  }

  public void setdId(Long dId) {
    this.dId = dId;
  }

  public Long getwId() {
    return wId;
  }

  public void setwId(Long wId) {
    this.wId = wId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OrderId that)) {
      return false;
    }
    return Objects.equal(oId, that.oId)
        && Objects.equal(cId, that.cId)
        && Objects.equal(dId, that.dId)
        && Objects.equal(wId, that.wId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(oId, cId, dId, wId);
  }

  @Override
  public String toString() {
    return "OrderId{" +
        "oId=" + oId +
        ", cId=" + cId +
        ", dId=" + dId +
        ", wId=" + wId +
        '}';
  }
}
