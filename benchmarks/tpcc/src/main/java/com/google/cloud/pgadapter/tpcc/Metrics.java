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
package com.google.cloud.pgadapter.tpcc;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.Arrays;
import java.util.List;

public class Metrics {
  static final String INSTRUMENTATION_SCOPE = "cloud.google.com/java";
  static final String ROUNDTRIP_LATENCY = "spanner/benchmarker/roundtrip_latencies";

  static final String ROUNDTRIP_LATENCY_DESCRIPTION =
      "Latency when the benchmark runner sends out a query and gets a response";

  static final String TPCC_NEWORDER_LATENCY = "spanner/benchmarker/tpcc_neworder_latencies";

  static final String TPCC_PAYMENT_LATENCY = "spanner/benchmarker/tpcc_payment_latencies";

  static final String TPCC_ORDERSTATUS_LATENCY = "spanner/benchmarker/tpcc_orderstatus_latencies";

  static final String TPCC_DELIVERY_LATENCY = "spanner/benchmarker/tpcc_delivery_latencies";

  static final String TPCC_STOCKLEVEL_LATENCY = "spanner/benchmarker/tpcc_stocklevel_latencies";

  static final String TPCC_TRANSACTION_DESCRIPTON = "Latency of TPCC transaction";

  private final LongHistogram latencies;

  private final LongHistogram newOrderLatencies;

  private final LongHistogram paymentLatencies;

  private final LongHistogram orderStatusLatencies;

  private final LongHistogram deliveryLatencies;

  private final LongHistogram stockLevelLatencies;
  private final Attributes attributes;

  public Metrics(OpenTelemetry openTelemetry, Attributes attributes) {
    Meter meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE);
    List<Long> RPC_MILLIS_BUCKET_BOUNDARIES =
        Arrays.asList(
            1L, 2L, 3L, 4L, 5L, 6L, 8L, 10L, 13L, 16L, 20L, 25L, 30L, 40L, 50L, 65L, 80L, 100L,
            130L, 160L, 200L, 250L, 300L, 400L, 500L, 650L, 800L, 1000L, 2000L, 5000L, 10000L,
            20000L, 50000L, 100000L);
    latencies =
        meter
            .histogramBuilder(ROUNDTRIP_LATENCY)
            .ofLongs()
            .setDescription(ROUNDTRIP_LATENCY_DESCRIPTION)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(RPC_MILLIS_BUCKET_BOUNDARIES)
            .build();
    newOrderLatencies =
        meter
            .histogramBuilder(TPCC_NEWORDER_LATENCY)
            .ofLongs()
            .setDescription(TPCC_TRANSACTION_DESCRIPTON)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(RPC_MILLIS_BUCKET_BOUNDARIES)
            .build();
    paymentLatencies =
        meter
            .histogramBuilder(TPCC_PAYMENT_LATENCY)
            .ofLongs()
            .setDescription(TPCC_TRANSACTION_DESCRIPTON)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(RPC_MILLIS_BUCKET_BOUNDARIES)
            .build();
    orderStatusLatencies =
        meter
            .histogramBuilder(TPCC_ORDERSTATUS_LATENCY)
            .ofLongs()
            .setDescription(TPCC_TRANSACTION_DESCRIPTON)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(RPC_MILLIS_BUCKET_BOUNDARIES)
            .build();
    deliveryLatencies =
        meter
            .histogramBuilder(TPCC_DELIVERY_LATENCY)
            .ofLongs()
            .setDescription(TPCC_TRANSACTION_DESCRIPTON)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(RPC_MILLIS_BUCKET_BOUNDARIES)
            .build();
    stockLevelLatencies =
        meter
            .histogramBuilder(TPCC_STOCKLEVEL_LATENCY)
            .ofLongs()
            .setDescription(TPCC_TRANSACTION_DESCRIPTON)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(RPC_MILLIS_BUCKET_BOUNDARIES)
            .build();
    this.attributes = attributes;
  }

  public void recordLatency(long value) {
    latencies.record(value, attributes);
  }

  public void recordNewOrderLatency(long value) {
    newOrderLatencies.record(value, attributes);
  }

  public void recordPaymentLatency(long value) {
    paymentLatencies.record(value, attributes);
  }

  public void recordOrderStatusLatency(long value) {
    orderStatusLatencies.record(value, attributes);
  }

  public void recordDeliveryLatency(long value) {
    deliveryLatencies.record(value, attributes);
  }

  public void recordStockLevelLatency(long value) {
    stockLevelLatencies.record(value, attributes);
  }
}
