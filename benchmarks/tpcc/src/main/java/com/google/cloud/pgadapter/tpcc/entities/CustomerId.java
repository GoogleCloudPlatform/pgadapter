package com.google.cloud.pgadapter.tpcc.entities;

import com.google.common.base.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
public class CustomerId implements Serializable {

  @Column(name = "c_id")
  private Long cId;

  @Column(name = "d_id")
  private Long dId;

  @Column(name = "w_id")
  private Long wId;

  public CustomerId(){

  }

  public CustomerId(long customerId, long districtId, long warehouseId) {
    this.cId = customerId;
    this.dId = districtId;
    this.wId = warehouseId;
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
    if (!(o instanceof CustomerId that)) {
      return false;
    }
    return Objects.equal(cId, that.cId)
        && Objects.equal(dId, that.dId)
        && Objects.equal(wId, that.wId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(cId, dId, wId);
  }

  @Override
  public String toString() {
    return "CustomerId{" +
        "cId=" + cId +
        ", dId=" + dId +
        ", wId=" + wId +
        '}';
  }
}
