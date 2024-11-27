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

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.sql.Timestamp;
import org.hibernate.annotations.DynamicUpdate;

@Entity
@Table(name = "customer")
public class Customer {

  @EmbeddedId private CustomerId id;

  @ManyToOne(fetch = FetchType.EAGER)
  @JoinColumns({
    @JoinColumn(
        name = "w_id",
        referencedColumnName = "w_id",
        insertable = false,
        updatable = false),
    @JoinColumn(name = "d_id", referencedColumnName = "d_id", insertable = false, updatable = false)
  })
  private District district;

  @Column(name = "c_first")
  private String first;

  @Column(name = "c_middle")
  private String middle;

  @Column(name = "c_last")
  private String last;

  @Column(name = "c_street_1")
  private String street1;

  @Column(name = "c_street_2")
  private String street2;

  @Column(name = "c_city")
  private String city;

  @Column(name = "c_state")
  private String state;

  @Column(name = "c_zip")
  private String zip;

  @Column(name = "c_phone")
  private String phone;

  @Column(name = "c_since")
  private Timestamp since;

  @Column(name = "c_credit")
  private String credit;

  @Column(name = "c_credit_lim")
  private Long creditLim;

  @Column(name = "c_discount")
  private BigDecimal discount;

  @Column(name = "c_balance")
  private BigDecimal balance;

  @Column(name = "c_ytd_payment")
  private BigDecimal ytdPayment;

  @Column(name = "c_payment_cnt")
  private Long paymentCnt;

  @Column(name = "c_delivery_cnt")
  private Long deliveryCnt;

  @Lob
  @Basic(fetch = FetchType.LAZY)
  @Column(name = "c_data")
  private String data;

  public CustomerId getId() {
    return id;
  }

  public void setId(CustomerId id) {
    this.id = id;
  }

  public District getDistrict() {
    return district;
  }

  public void setDistrict(District district) {
    this.district = district;
  }

  public String getFirst() {
    return first;
  }

  public void setFirst(String first) {
    this.first = first;
  }

  public String getMiddle() {
    return middle;
  }

  public void setMiddle(String middle) {
    this.middle = middle;
  }

  public String getLast() {
    return last;
  }

  public void setLast(String last) {
    this.last = last;
  }

  public String getStreet1() {
    return street1;
  }

  public void setStreet1(String street1) {
    this.street1 = street1;
  }

  public String getStreet2() {
    return street2;
  }

  public void setStreet2(String street2) {
    this.street2 = street2;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getZip() {
    return zip;
  }

  public void setZip(String zip) {
    this.zip = zip;
  }

  public String getPhone() {
    return phone;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public Timestamp getSince() {
    return since;
  }

  public void setSince(Timestamp since) {
    this.since = since;
  }

  public String getCredit() {
    return credit;
  }

  public void setCredit(String credit) {
    this.credit = credit;
  }

  public Long getCreditLim() {
    return creditLim;
  }

  public void setCreditLim(Long creditLim) {
    this.creditLim = creditLim;
  }

  public BigDecimal getDiscount() {
    return discount;
  }

  public void setDiscount(BigDecimal discount) {
    this.discount = discount;
  }

  public BigDecimal getBalance() {
    return balance;
  }

  public void setBalance(BigDecimal balance) {
    this.balance = balance;
  }

  public BigDecimal getYtdPayment() {
    return ytdPayment;
  }

  public void setYtdPayment(BigDecimal ytdPayment) {
    this.ytdPayment = ytdPayment;
  }

  public Long getPaymentCnt() {
    return paymentCnt;
  }

  public void setPaymentCnt(Long paymentCnt) {
    this.paymentCnt = paymentCnt;
  }

  public Long getDeliveryCnt() {
    return deliveryCnt;
  }

  public void setDeliveryCnt(Long deliveryCnt) {
    this.deliveryCnt = deliveryCnt;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }
}
