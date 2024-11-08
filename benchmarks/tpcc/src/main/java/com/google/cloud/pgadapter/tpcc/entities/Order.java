package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.sql.Timestamp;

@Entity
@Table(name = "orders")
public class Order {

  @Id
  @Column(name = "o_id")
  private Long oId;

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "w_id", referencedColumnName = "w_id"),
    @JoinColumn(name = "d_id", referencedColumnName = "d_id"),
    @JoinColumn(name = "c_id", referencedColumnName = "c_id")
  })
  private Customer customer;

  @Column(name = "o_entry_d")
  private Timestamp oEntryD;

  @Column(name = "o_carrier_id")
  private Long oCarrierId;

  @Column(name = "o_ol_cnt")
  private Long oOlCnt;

  @Column(name = "o_all_local")
  private Long oAllLocal;

  public Long getoId() {
    return oId;
  }

  public void setoId(Long oId) {
    this.oId = oId;
  }

  public Customer getCustomer() {
    return customer;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  public Timestamp getoEntryD() {
    return oEntryD;
  }

  public void setoEntryD(Timestamp oEntryD) {
    this.oEntryD = oEntryD;
  }

  public Long getoCarrierId() {
    return oCarrierId;
  }

  public void setoCarrierId(Long oCarrierId) {
    this.oCarrierId = oCarrierId;
  }

  public Long getoOlCnt() {
    return oOlCnt;
  }

  public void setoOlCnt(Long oOlCnt) {
    this.oOlCnt = oOlCnt;
  }

  public Long getoAllLocal() {
    return oAllLocal;
  }

  public void setoAllLocal(Long oAllLocal) {
    this.oAllLocal = oAllLocal;
  }
}
