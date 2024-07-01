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
package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.cloud.Timestamp;
import com.google.cloud.pgadapter.tpcc.LastNameGenerator;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;

class CustomerRowProducer extends AbstractRowProducer {
  private static final String TABLE = "customer";
  private static final String COLUMNS =
      """
    c_id,
    d_id,
    w_id,
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

  CustomerRowProducer(DataLoadStatus status, long warehouseId, long districtId, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incCustomer);
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
            getLastName(rowIndex),
            getRandomStreet(1),
            getRandomStreet(2),
            getRandomCity(),
            getRandomState(),
            getRandomZip(),
            getRandomPhone(),
            nowAsString(), // Use a timestamp as string.
            getCredit(),
            getCreditLimit(),
            getDiscount(),
            getBalance(),
            getYtdPayment(),
            getPaymentCount(),
            getDeliveryCount(),
            getData()));
  }

  @Override
  List<ImmutableList> createRowsAsList(long rowIndex) {
    return Arrays.asList(
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(districtId),
            String.valueOf(warehouseId),
            getRandomName(),
            getRandomString(2),
            getLastName(rowIndex),
            getRandomStreet(1),
            getRandomStreet(2),
            getRandomCity(),
            getRandomState(),
            getRandomZip(),
            getRandomPhone(),
            Timestamp.now(),
            getCredit(),
            getCreditLimit(),
            getDiscount(),
            getBalance(),
            getYtdPayment(),
            getPaymentCount(),
            getDeliveryCount(),
            getData()));
  }

  String getLastName(long rowIndex) {
    return LastNameGenerator.generateLastName(this.random, rowIndex);
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
