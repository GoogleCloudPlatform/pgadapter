package com.google.cloud.pgadapter.tpcc.entities;

import com.google.common.base.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
public class StockId implements Serializable {

  private static final long serialVersionUID = 1L;

  @Column(name = "s_i_id")
  private Long sIId;

  @Column(name = "w_id")
  private Long wId;

  public StockId() {
  }

  public StockId(long orderLineItemId, long supplyWarehouse) {
    this.sIId = orderLineItemId;
    this.wId = supplyWarehouse;
  }

  public Long getsIId() {
    return sIId;
  }

  public void setsIId(Long sIId) {
    this.sIId = sIId;
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
    if (!(o instanceof StockId stockId)) {
      return false;
    }
    return Objects.equal(sIId, stockId.sIId) && Objects.equal(wId, stockId.wId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sIId, wId);
  }

  @Override
  public String toString() {
    return "StockId{" + "sIId=" + sIId + ", wId=" + wId + '}';
  }
}
