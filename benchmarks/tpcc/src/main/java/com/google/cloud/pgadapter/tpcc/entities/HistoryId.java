package com.google.cloud.pgadapter.tpcc.entities;


import com.google.common.base.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.sql.Timestamp;

@Embeddable
public class HistoryId implements Serializable {

  @Column(name = "c_id")
  private Long cId;

  @Column(name = "d_id")
  private Long dId;

  @Column(name = "w_id")
  private Long wId;

  @Column(name = "h_d_id")
  private Long hDId;

  @Column(name = "h_w_id")
  private Long hWId;

  @Column(name = "h_date")
  private Timestamp hDate;

  public HistoryId() {
  }

  public HistoryId(long customerId, long customerDistrictId, long customerWarehouseId,
      long districtId, long warehouseId, Timestamp timestamp) {
    this.cId = customerId;
    this.dId = customerDistrictId;
    this.wId = customerWarehouseId;
    this.hDId = districtId;
    this.hWId = warehouseId;
    this.hDate = timestamp;
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

  public Long gethDId() {
    return hDId;
  }

  public void sethDId(Long hDId) {
    this.hDId = hDId;
  }

  public Long gethWId() {
    return hWId;
  }

  public void sethWId(Long hWId) {
    this.hWId = hWId;
  }

  public Timestamp gethDate() {
    return hDate;
  }

  public void sethDate(Timestamp hDate) {
    this.hDate = hDate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HistoryId historyId)) {
      return false;
    }
    return Objects.equal(cId, historyId.cId)
        && Objects.equal(dId, historyId.dId)
        && Objects.equal(wId, historyId.wId)
        && Objects.equal(hDId, historyId.hDId)
        && Objects.equal(hWId, historyId.hWId)
        && Objects.equal(hDate, historyId.hDate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(cId, dId, wId, hDId, hWId, hDate);
  }

  @Override
  public String toString() {
    return "HistoryId{" +
        "cId=" + cId +
        ", dId=" + dId +
        ", wId=" + wId +
        ", hDId=" + hDId +
        ", hWId=" + hWId +
        ", hDate=" + hDate +
        '}';
  }
}
