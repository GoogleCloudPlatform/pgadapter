package com.google.cloud.pgadapter.tpcc.entities;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.sql.Timestamp;

@Entity
@Table(name = "order_line")
public class OrderLine {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id; // You might want to add a surrogate key

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "w_id", referencedColumnName = "w_id"),
    @JoinColumn(name = "d_id", referencedColumnName = "d_id"),
    @JoinColumn(name = "c_id", referencedColumnName = "c_id"),
    @JoinColumn(name = "o_id", referencedColumnName = "o_id")
  })
  private Order order;

  @Column(name = "ol_number")
  private Long olNumber;

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "ol_supply_w_id", referencedColumnName = "w_id"),
    @JoinColumn(name = "ol_i_id", referencedColumnName = "s_i_id")
  })
  private Stock stock;

  @Column(name = "ol_delivery_d")
  private Timestamp olDeliveryD;

  @Column(name = "ol_quantity")
  private Long olQuantity;

  @Column(name = "ol_amount", precision = 12, scale = 4)
  private BigDecimal olAmount;

  @Column(name = "ol_dist_info", length = 24)
  private String olDistInfo;

  // Getters and setters
}
