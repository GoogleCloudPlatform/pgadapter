package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "new_orders")
public class NewOrder {

  @Id
  @Column(name = "o_id")
  private Long oId;

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "w_id", referencedColumnName = "w_id"),
    @JoinColumn(name = "d_id", referencedColumnName = "d_id"),
    @JoinColumn(name = "c_id", referencedColumnName = "c_id")
  })
  private Order order;

  public Long getoId() {
    return oId;
  }

  public void setoId(Long oId) {
    this.oId = oId;
  }

  public Order getOrder() {
    return order;
  }

  public void setOrder(Order order) {
    this.order = order;
  }
}
