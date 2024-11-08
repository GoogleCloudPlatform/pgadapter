package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.*;
import java.math.BigDecimal;

@Entity
@Table(name = "stock")
public class Stock {

  @Id
  @Column(name = "s_i_id")
  private Long sIId;

  @ManyToOne
  @JoinColumn(name = "w_id", referencedColumnName = "w_id")
  private Warehouse warehouse;

  @Column(name = "s_quantity")
  private Long sQuantity;

  @Column(name = "s_dist_01", length = 24)
  private String sDist01;

  @Column(name = "s_dist_02", length = 24)
  private String sDist02;

  @Column(name = "s_dist_03", length = 24)
  private String sDist03;

  @Column(name = "s_dist_04", length = 24)
  private String sDist04;

  @Column(name = "s_dist_05", length = 24)
  private String sDist05;

  @Column(name = "s_dist_06", length = 24)
  private String sDist06;

  @Column(name = "s_dist_07", length = 24)
  private String sDist07;

  @Column(name = "s_dist_08", length = 24)
  private String sDist08;

  @Column(name = "s_dist_09", length = 24)
  private String sDist09;

  @Column(name = "s_dist_10", length = 24)
  private String sDist10;

  @Column(name = "s_ytd", precision = 12, scale = 4)
  private BigDecimal sYtd;

  @Column(name = "s_order_cnt")
  private Long sOrderCnt;

  @Column(name = "s_remote_cnt")
  private Long sRemoteCnt;

  @Column(name = "s_data", length = 50)
  private String sData;

  public Long getsIId() {
    return sIId;
  }

  public void setsIId(Long sIId) {
    this.sIId = sIId;
  }

  public Warehouse getWarehouse() {
    return warehouse;
  }

  public void setWarehouse(Warehouse warehouse) {
    this.warehouse = warehouse;
  }

  public Long getsQuantity() {
    return sQuantity;
  }

  public void setsQuantity(Long sQuantity) {
    this.sQuantity = sQuantity;
  }

  public String getsDist01() {
    return sDist01;
  }

  public void setsDist01(String sDist01) {
    this.sDist01 = sDist01;
  }

  public String getsDist02() {
    return sDist02;
  }

  public void setsDist02(String sDist02) {
    this.sDist02 = sDist02;
  }

  public String getsDist03() {
    return sDist03;
  }

  public void setsDist03(String sDist03) {
    this.sDist03 = sDist03;
  }

  public String getsDist04() {
    return sDist04;
  }

  public void setsDist04(String sDist04) {
    this.sDist04 = sDist04;
  }

  public String getsDist05() {
    return sDist05;
  }

  public void setsDist05(String sDist05) {
    this.sDist05 = sDist05;
  }

  public String getsDist06() {
    return sDist06;
  }

  public void setsDist06(String sDist06) {
    this.sDist06 = sDist06;
  }

  public String getsDist07() {
    return sDist07;
  }

  public void setsDist07(String sDist07) {
    this.sDist07 = sDist07;
  }

  public String getsDist08() {
    return sDist08;
  }

  public void setsDist08(String sDist08) {
    this.sDist08 = sDist08;
  }

  public String getsDist09() {
    return sDist09;
  }

  public void setsDist09(String sDist09) {
    this.sDist09 = sDist09;
  }

  public String getsDist10() {
    return sDist10;
  }

  public void setsDist10(String sDist10) {
    this.sDist10 = sDist10;
  }

  public BigDecimal getsYtd() {
    return sYtd;
  }

  public void setsYtd(BigDecimal sYtd) {
    this.sYtd = sYtd;
  }

  public Long getsOrderCnt() {
    return sOrderCnt;
  }

  public void setsOrderCnt(Long sOrderCnt) {
    this.sOrderCnt = sOrderCnt;
  }

  public Long getsRemoteCnt() {
    return sRemoteCnt;
  }

  public void setsRemoteCnt(Long sRemoteCnt) {
    this.sRemoteCnt = sRemoteCnt;
  }

  public String getsData() {
    return sData;
  }

  public void setsData(String sData) {
    this.sData = sData;
  }
}
