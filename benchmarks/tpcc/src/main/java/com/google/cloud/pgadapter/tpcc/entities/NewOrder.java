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

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "new_orders")
public class NewOrder {

  @EmbeddedId private OrderId id;

  @OneToOne(fetch = FetchType.EAGER)
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

  public OrderId getId() {
    return id;
  }

  public void setId(OrderId id) {
    this.id = id;
  }

  public Order getOrder() {
    return order;
  }

  public void setOrder(Order order) {
    this.order = order;
  }
}
