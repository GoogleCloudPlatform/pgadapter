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
import java.sql.Timestamp;
import org.hibernate.annotations.DynamicUpdate;

@Entity
@Table(name = "order_line")
public class OrderLine {
  @EmbeddedId private OrderLineId id;

  @Column(name = "ol_i_id", insertable = false, updatable = false)
  private Long olIId;

  @Column(name = "ol_supply_w_id", insertable = false, updatable = false)
  private Long olSupplyWId;

  @Column(name = "ol_delivery_d")
  private Timestamp olDeliveryD;

  @Column(name = "ol_quantity")
  private Long olQuantity;

  @Column(name = "ol_amount")
  private BigDecimal olAmount;

  @Column(name = "ol_dist_info")
  private String olDistInfo;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumns({
    @JoinColumn(
        name = "w_id",
        referencedColumnName = "w_id",
        insertable = false,
        updatable = false),
    @JoinColumn(
        name = "d_id",
        referencedColumnName = "d_id",
        insertable = false,
        updatable = false),
    @JoinColumn(
        name = "c_id",
        referencedColumnName = "c_id",
        insertable = false,
        updatable = false),
    @JoinColumn(name = "o_id", referencedColumnName = "o_id", insertable = false, updatable = false)
  })
  private Order order;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumns({
    @JoinColumn(name = "ol_supply_w_id", referencedColumnName = "w_id"),
    @JoinColumn(name = "ol_i_id", referencedColumnName = "s_i_id")
  })
  private Stock stock;

  public OrderLineId getId() {
    return id;
  }

  public void setId(OrderLineId id) {
    this.id = id;
  }

  public Long getOlIId() {
    return olIId;
  }

  public void setOlIId(Long olIId) {
    this.olIId = olIId;
  }

  public Long getOlSupplyWId() {
    return olSupplyWId;
  }

  public void setOlSupplyWId(Long olSupplyWId) {
    this.olSupplyWId = olSupplyWId;
  }

  public Timestamp getOlDeliveryD() {
    return olDeliveryD;
  }

  public void setOlDeliveryD(Timestamp olDeliveryD) {
    this.olDeliveryD = olDeliveryD;
  }

  public Long getOlQuantity() {
    return olQuantity;
  }

  public void setOlQuantity(Long olQuantity) {
    this.olQuantity = olQuantity;
  }

  public BigDecimal getOlAmount() {
    return olAmount;
  }

  public void setOlAmount(BigDecimal olAmount) {
    this.olAmount = olAmount;
  }

  public String getOlDistInfo() {
    return olDistInfo;
  }

  public void setOlDistInfo(String olDistInfo) {
    this.olDistInfo = olDistInfo;
  }

  public Order getOrder() {
    return order;
  }

  public void setOrder(Order order) {
    this.order = order;
  }

  public Stock getStock() {
    return stock;
  }

  public void setStock(Stock stock) {
    this.stock = stock;
  }
}
