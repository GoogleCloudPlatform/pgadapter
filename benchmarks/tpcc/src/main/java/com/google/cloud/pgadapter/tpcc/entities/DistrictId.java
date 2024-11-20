package com.google.cloud.pgadapter.tpcc.entities;

import com.google.common.base.Objects;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
public class DistrictId implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  @Column(name = "w_id")
  private Long wId;

  @Column(name = "d_id")
  private Long dId;

  public DistrictId() {}

  public DistrictId(Long dId, Long wId) {
    this.dId = dId;
    this.wId = wId;
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
  public String toString() {
    return "DistrictId{" + "dId=" + dId + ", wId=" + wId + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DistrictId that)) {
      return false;
    }
    return Objects.equal(dId, that.dId) && Objects.equal(wId, that.wId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dId, wId);
  }
}
