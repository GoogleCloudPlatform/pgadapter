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

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.sql.Timestamp;
import java.util.List;

@Entity
@Table(name = "orders")
public class Order {
  @EmbeddedId
  private OrderId id;


  @OneToMany(mappedBy = "order", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
  private List<OrderLine> orderLines;

  public Customer getCustomer() {
    return customer;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumns({
      @JoinColumn(name = "w_id", referencedColumnName = "w_id", insertable = false, updatable = false),
      @JoinColumn(name = "d_id", referencedColumnName = "d_id", insertable = false, updatable = false),
      @JoinColumn(name = "c_id", referencedColumnName = "c_id", insertable = false, updatable = false)
  })
  private Customer customer;

  public List<OrderLine> getOrderLines() {
    return orderLines;
  }

  public void setOrderLines(List<OrderLine> orderLines) {
    this.orderLines = orderLines;
  }

  @Column(name = "o_entry_d")
  private Timestamp entryD;

  @Column(name = "o_carrier_id")
  private Long carrierId;

  @Column(name = "o_ol_cnt")
  private Integer olCnt;

  @Column(name = "o_all_local")
  private Integer allLocal;

  public OrderId getId() {
    return id;
  }

  public void setId(OrderId id) {
    this.id = id;
  }

  public Timestamp getEntryD() {
    return entryD;
  }

  public void setEntryD(Timestamp entryD) {
    this.entryD = entryD;
  }

  public Long getCarrierId() {
    return carrierId;
  }

  public void setCarrierId(Long carrierId) {
    this.carrierId = carrierId;
  }

  public Integer getOlCnt() {
    return olCnt;
  }

  public void setOlCnt(Integer olCnt) {
    this.olCnt = olCnt;
  }

  public Integer getAllLocal() {
    return allLocal;
  }

  public void setAllLocal(Integer allLocal) {
    this.allLocal = allLocal;
  }
}
