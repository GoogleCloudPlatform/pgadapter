package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;

class CustomerRowProducer extends AbstractRowProducer {
  private static final String TABLE = "customer";
  private static final String COLUMNS =
      """
    c_id,
    c_d_id,
    c_w_id,
    c_first,
    c_middle,
    c_last,
    c_street_1,
    c_street_2,
    c_city,
    c_state,
    c_zip,
    c_phone,
    c_since,
    c_credit,
    c_credit_lim,
    c_discount,
    c_balance,
    c_ytd_payment,
    c_payment_cnt,
    c_delivery_cnt,
    c_data
    """;

  private final long warehouseId;

  private final long districtId;

  CustomerRowProducer(long warehouseId, long districtId, long rowCount) {
    super(TABLE, COLUMNS, rowCount);
    this.warehouseId = warehouseId;
    this.districtId = districtId;
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(districtId),
            String.valueOf(warehouseId),
            getRandomName(),
            getRandomString(2),
            getRandomName(),
            getRandomStreet(1),
            getRandomStreet(2),
            getRandomCity(),
            getRandomState(),
            getRandomZip(),
            getRandomPhone(),
            now(),
            getCredit(),
            getCreditLimit(),
            getDiscount(),
            getBalance(),
            getYtdPayment(),
            getPaymentCount(),
            getDeliveryCount(),
            getData()));
  }

  String getCredit() {
    return quote(random.nextBoolean() ? "GC" : "BC");
  }

  String getCreditLimit() {
    return "50000";
  }

  String getDiscount() {
    return getRandomDecimal(2);
  }

  String getBalance() {
    return "-10.0";
  }

  String getYtdPayment() {
    return "10.0";
  }

  String getPaymentCount() {
    return "1";
  }

  String getDeliveryCount() {
    return "0";
  }

  String getData() {
    return getRandomString(random.nextInt(100) + 50);
  }
}
