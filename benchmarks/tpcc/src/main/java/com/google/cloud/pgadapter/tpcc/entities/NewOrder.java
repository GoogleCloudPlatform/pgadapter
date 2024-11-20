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

  @EmbeddedId
  private OrderId id;

  @OneToOne(fetch = FetchType.LAZY)
  @JoinColumns({
      @JoinColumn(name = "w_id", referencedColumnName = "w_id", insertable = false, updatable = false),
      @JoinColumn(name = "d_id", referencedColumnName = "d_id", insertable = false, updatable = false),
      @JoinColumn(name = "c_id", referencedColumnName = "c_id", insertable = false, updatable = false),
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
