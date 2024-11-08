package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;

@Entity
@Table(name = "district")
public class District {

  @EmbeddedId private DistrictId id;

  @ManyToOne(fetch = FetchType.LAZY)
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

  @Override
  public String toString() {
    return "District{"
        + "id="
        + id
        + ", warehouse="
        + warehouse
        + ", dName='"
        + dName
        + '\''
        + ", dStreet1='"
        + dStreet1
        + '\''
        + ", dStreet2='"
        + dStreet2
        + '\''
        + ", dCity='"
        + dCity
        + '\''
        + ", dState='"
        + dState
        + '\''
        + ", dZip='"
        + dZip
        + '\''
        + ", dTax="
        + dTax
        + ", dYtd="
        + dYtd
        + ", dNextOId="
        + dNextOId
        + '}';
  }
}
