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

import jakarta.persistence.*;
import java.math.BigDecimal;
import org.hibernate.annotations.DynamicUpdate;

@Entity
@DynamicUpdate
@Table(name = "stock")
public class Stock {

  @EmbeddedId private StockId id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "w_id", insertable = false, updatable = false)
  private Warehouse warehouse;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "s_i_id", insertable = false, updatable = false)
  private Item item;

  @Column(name = "s_quantity")
  private Long quantity;

  @Column(name = "s_dist_01")
  private String dist01;

  @Column(name = "s_dist_02")
  private String dist02;

  @Column(name = "s_dist_03")
  private String dist03;

  @Column(name = "s_dist_04")
  private String dist04;

  @Column(name = "s_dist_05")
  private String dist05;

  @Column(name = "s_dist_06")
  private String dist06;

  @Column(name = "s_dist_07")
  private String dist07;

  @Column(name = "s_dist_08")
  private String dist08;

  @Column(name = "s_dist_09")
  private String dist09;

  @Column(name = "s_dist_10")
  private String dist10;

  @Column(name = "s_ytd")
  private BigDecimal ytd;

  @Column(name = "s_order_cnt")
  private Long orderCnt;

  @Column(name = "s_remote_cnt")
  private Long remoteCnt;

  @Column(name = "s_data")
  private String data;

  public StockId getId() {
    return id;
  }

  public void setId(StockId id) {
    this.id = id;
  }

  public Warehouse getWarehouse() {
    return warehouse;
  }

  public void setWarehouse(Warehouse warehouse) {
    this.warehouse = warehouse;
  }

  public Item getItem() {
    return item;
  }

  public void setItem(Item item) {
    this.item = item;
  }

  public Long getQuantity() {
    return quantity;
  }

  public void setQuantity(Long quantity) {
    this.quantity = quantity;
  }

  public String getDist01() {
    return dist01;
  }

  public void setDist01(String dist01) {
    this.dist01 = dist01;
  }

  public String getDist02() {
    return dist02;
  }

  public void setDist02(String dist02) {
    this.dist02 = dist02;
  }

  public String getDist03() {
    return dist03;
  }

  public void setDist03(String dist03) {
    this.dist03 = dist03;
  }

  public String getDist04() {
    return dist04;
  }

  public void setDist04(String dist04) {
    this.dist04 = dist04;
  }

  public String getDist05() {
    return dist05;
  }

  public void setDist05(String dist05) {
    this.dist05 = dist05;
  }

  public String getDist06() {
    return dist06;
  }

  public void setDist06(String dist06) {
    this.dist06 = dist06;
  }

  public String getDist07() {
    return dist07;
  }

  public void setDist07(String dist07) {
    this.dist07 = dist07;
  }

  public String getDist08() {
    return dist08;
  }

  public void setDist08(String dist08) {
    this.dist08 = dist08;
  }

  public String getDist09() {
    return dist09;
  }

  public void setDist09(String dist09) {
    this.dist09 = dist09;
  }

  public String getDist10() {
    return dist10;
  }

  public void setDist10(String dist10) {
    this.dist10 = dist10;
  }

  public BigDecimal getYtd() {
    return ytd;
  }

  public void setYtd(BigDecimal ytd) {
    this.ytd = ytd;
  }

  public Long getOrderCnt() {
    return orderCnt;
  }

  public void setOrderCnt(Long orderCnt) {
    this.orderCnt = orderCnt;
  }

  public Long getRemoteCnt() {
    return remoteCnt;
  }

  public void setRemoteCnt(Long remoteCnt) {
    this.remoteCnt = remoteCnt;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }
}
