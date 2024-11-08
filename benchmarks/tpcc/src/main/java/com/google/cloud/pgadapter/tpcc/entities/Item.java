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
