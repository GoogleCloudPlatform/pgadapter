package com.google.cloud.pgadapter.tpcc.entities;

import com.google.common.base.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
public class OrderLineId implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  @Column(name = "o_id")
  private Long oId;

  @Column(name = "c_id")
  private Long cId;

  @Column(name = "d_id")
  private Long dId;

  @Column(name = "w_id")
  private Long wId;

  @Column(name = "ol_number")
  private Long olNumber;

  public OrderLineId() {
  }

  public OrderLineId(long districtNextOrderId, long customerId, long districtId, long warehouseId,
      long l) {
    this.oId = districtNextOrderId;
    this.cId = customerId;
    this.dId = districtId;
    this.wId = warehouseId;
    this.olNumber = l;
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

  public Long getOlNumber() {
    return olNumber;
  }

  public void setOlNumber(Long olNumber) {
    this.olNumber = olNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OrderLineId that)) {
      return false;
    }
    return Objects.equal(oId, that.oId)
        && Objects.equal(cId, that.cId)
        && Objects.equal(dId, that.dId)
        && Objects.equal(wId, that.wId)
        && Objects.equal(olNumber, that.olNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(oId, cId, dId, wId, olNumber);
  }

  @Override
  public String toString() {
    return "OrderLineId{"
        + "oId="
        + oId
        + ", cId="
        + cId
        + ", dId="
        + dId
        + ", wId="
        + wId
        + ", olNumber="
        + olNumber
        + '}';
  }
}
