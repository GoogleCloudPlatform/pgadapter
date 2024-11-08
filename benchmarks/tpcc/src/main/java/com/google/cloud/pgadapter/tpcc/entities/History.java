package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.sql.Timestamp;

@Entity
@Table(name = "history")
public class History {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id; // You might want to add a surrogate key

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "c_id", referencedColumnName = "c_id"),
    @JoinColumn(name = "d_id", referencedColumnName = "d_id"),
    @JoinColumn(name = "w_id", referencedColumnName = "w_id")
  })
  private Customer customer;

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "h_w_id", referencedColumnName = "w_id"),
    @JoinColumn(name = "h_d_id", referencedColumnName = "d_id")
  })
  private District district;

  @Column(name = "h_date")
  private Timestamp hDate;

  @Column(name = "h_amount", precision = 12, scale = 4)
  private BigDecimal hAmount;

  @Column(name = "h_data", length = 24)
  private String hData;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Customer getCustomer() {
    return customer;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  public District getDistrict() {
    return district;
  }

  public void setDistrict(District district) {
    this.district = district;
  }

  public Timestamp gethDate() {
    return hDate;
  }

  public void sethDate(Timestamp hDate) {
    this.hDate = hDate;
  }

  public BigDecimal gethAmount() {
    return hAmount;
  }

  public void sethAmount(BigDecimal hAmount) {
    this.hAmount = hAmount;
  }

  public String gethData() {
    return hData;
  }

  public void sethData(String hData) {
    this.hData = hData;
  }
}
