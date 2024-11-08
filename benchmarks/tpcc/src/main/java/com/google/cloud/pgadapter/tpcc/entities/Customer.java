package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.sql.Timestamp;

@Entity
@Table(name = "customer")
public class Customer {

  @Id
  @Column(name = "c_id")
  private Long cId;

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "w_id", referencedColumnName = "w_id"),
    @JoinColumn(name = "d_id", referencedColumnName = "d_id")
  })
  private District district;

  @Column(name = "c_first", length = 16)
  private String cFirst;

  @Column(name = "c_middle", length = 2)
  private String cMiddle;

  @Column(name = "c_last", length = 16)
  private String cLast;

  @Column(name = "c_street_1", length = 20)
  private String cStreet1;

  @Column(name = "c_street_2", length = 20)
  private String cStreet2;

  @Column(name = "c_city", length = 20)
  private String cCity;

  @Column(name = "c_state", length = 2)
  private String cState;

  @Column(name = "c_zip", length = 9)
  private String cZip;

  @Column(name = "c_phone", length = 16)
  private String cPhone;

  @Column(name = "c_since")
  private Timestamp cSince;

  @Column(name = "c_credit", length = 2)
  private String cCredit;

  @Column(name = "c_credit_lim")
  private Long cCreditLim;

  @Column(name = "c_discount", precision = 12, scale = 4)
  private BigDecimal cDiscount;

  @Column(name = "c_balance", precision = 12, scale = 4)
  private BigDecimal cBalance;

  @Column(name = "c_ytd_payment", precision = 12, scale = 4)
  private BigDecimal cYtdPayment;

  @Column(name = "c_payment_cnt")
  private Long cPaymentCnt;

  @Column(name = "c_delivery_cnt")
  private Long cDeliveryCnt;

  @Column(name = "c_data", columnDefinition = "TEXT")
  private String cData;

  public Long getcId() {
    return cId;
  }

  public void setcId(Long cId) {
    this.cId = cId;
  }

  public District getDistrict() {
    return district;
  }

  public void setDistrict(District district) {
    this.district = district;
  }

  public String getcFirst() {
    return cFirst;
  }

  public void setcFirst(String cFirst) {
    this.cFirst = cFirst;
  }

  public String getcMiddle() {
    return cMiddle;
  }

  public void setcMiddle(String cMiddle) {
    this.cMiddle = cMiddle;
  }

  public String getcLast() {
    return cLast;
  }

  public void setcLast(String cLast) {
    this.cLast = cLast;
  }

  public String getcStreet1() {
    return cStreet1;
  }

  public void setcStreet1(String cStreet1) {
    this.cStreet1 = cStreet1;
  }

  public String getcStreet2() {
    return cStreet2;
  }

  public void setcStreet2(String cStreet2) {
    this.cStreet2 = cStreet2;
  }

  public String getcCity() {
    return cCity;
  }

  public void setcCity(String cCity) {
    this.cCity = cCity;
  }

  public String getcState() {
    return cState;
  }

  public void setcState(String cState) {
    this.cState = cState;
  }

  public String getcZip() {
    return cZip;
  }

  public void setcZip(String cZip) {
    this.cZip = cZip;
  }

  public String getcPhone() {
    return cPhone;
  }

  public void setcPhone(String cPhone) {
    this.cPhone = cPhone;
  }

  public Timestamp getcSince() {
    return cSince;
  }

  public void setcSince(Timestamp cSince) {
    this.cSince = cSince;
  }

  public String getcCredit() {
    return cCredit;
  }

  public void setcCredit(String cCredit) {
    this.cCredit = cCredit;
  }

  public Long getcCreditLim() {
    return cCreditLim;
  }

  public void setcCreditLim(Long cCreditLim) {
    this.cCreditLim = cCreditLim;
  }

  public BigDecimal getcDiscount() {
    return cDiscount;
  }

  public void setcDiscount(BigDecimal cDiscount) {
    this.cDiscount = cDiscount;
  }

  public BigDecimal getcBalance() {
    return cBalance;
  }

  public void setcBalance(BigDecimal cBalance) {
    this.cBalance = cBalance;
  }

  public BigDecimal getcYtdPayment() {
    return cYtdPayment;
  }

  public void setcYtdPayment(BigDecimal cYtdPayment) {
    this.cYtdPayment = cYtdPayment;
  }

  public Long getcPaymentCnt() {
    return cPaymentCnt;
  }

  public void setcPaymentCnt(Long cPaymentCnt) {
    this.cPaymentCnt = cPaymentCnt;
  }

  public Long getcDeliveryCnt() {
    return cDeliveryCnt;
  }

  public void setcDeliveryCnt(Long cDeliveryCnt) {
    this.cDeliveryCnt = cDeliveryCnt;
  }

  public String getcData() {
    return cData;
  }

  public void setcData(String cData) {
    this.cData = cData;
  }
}
