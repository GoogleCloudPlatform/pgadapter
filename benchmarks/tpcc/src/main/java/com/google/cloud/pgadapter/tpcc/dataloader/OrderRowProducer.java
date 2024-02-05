package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.LongStream;

class OrderRowProducer extends AbstractOrderedIdRowProducer {
  static class DistrictId {
    final long warehouse;
    final long district;

    DistrictId(long warehouse, long district) {
      this.warehouse = warehouse;
      this.district = district;
    }

    @Override
    public int hashCode() {
      return Objects.hash(warehouse, district);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof DistrictId other)) {
        return false;
      }
      return this.warehouse == other.warehouse && this.district == other.district;
    }
  }

  public static final ConcurrentMap<DistrictId, ImmutableList<Long>> CUSTOMER_IDS =
      new ConcurrentHashMap<>();

  private static final String TABLE = "orders";
  private static final String COLUMNS =
      """
    o_id,
    d_id,
    w_id,
    c_id,
    o_entry_d,
    o_carrier_id,
    o_ol_cnt,
    o_all_local
    """;

  private final long warehouseId;

  private final long districtId;

  private final ImmutableList<Long> customerIds;

  OrderRowProducer(
      DataLoadStatus status, long warehouseId, long districtId, int customerCount, long rowCount) {
    super(TABLE, COLUMNS, rowCount, status::incOrders);
    this.warehouseId = warehouseId;
    this.districtId = districtId;
    ArrayList<Long> customerIds =
        Lists.newArrayList(LongStream.range(0L, customerCount).iterator());
    Collections.shuffle(customerIds);
    this.customerIds = ImmutableList.copyOf(customerIds);
    CUSTOMER_IDS.put(new DistrictId(warehouseId, districtId), this.customerIds);
  }

  @Override
  String createRow(long rowIndex) {
    return String.join(
        ",",
        ImmutableList.of(
            getId(rowIndex),
            String.valueOf(districtId),
            String.valueOf(warehouseId),
            getCustomerId(rowIndex),
            now(),
            getRandomCarrierId(rowIndex),
            getOrderLineCount(rowIndex),
            getAllLocal()));
  }

  String getCustomerId(long rowIndex) {
    return String.valueOf(Long.reverse(customerIds.get((int) rowIndex)));
  }

  String getRandomCarrierId(long rowIndex) {
    // NOTE: Empty string == NULL
    return rowIndex % 3L == 0 ? "" : String.valueOf(random.nextInt(10));
  }

  String getOrderLineCount(long rowIndex) {
    return String.valueOf(Math.abs(Objects.hash(warehouseId, districtId, rowIndex) % 11) + 5);
  }

  String getAllLocal() {
    return "1";
  }
}
