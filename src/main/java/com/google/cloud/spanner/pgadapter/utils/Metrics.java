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
package com.google.cloud.spanner.pgadapter.utils;

import com.google.api.core.InternalApi;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.ArrayList;
import java.util.List;

@InternalApi
public class Metrics {
  static final String INSTRUMENTATION_SCOPE = "cloud.google.com/java";
  static final String SPANNER_CLIENT_LIB_LATENCY = "spanner/pgadapter/client_lib_latencies";
  static final String SPANNER_CLIENT_LIB_LATENCY_DESCRIPTION =
      "Latency when the client library receives a call and returns a response";
  static final String PGADAPTER_LATENCY = "spanner/pgadapter/roundtrip_latencies";
  static final String PGADAPTER_LATENCY_DESCRIPTION =
      "Latency between PGAdapter receiving a statement from the client and PGAdapter returning the last row of the response to the client";

  private final DoubleHistogram spannerClientLibLatencies;
  private final DoubleHistogram pgadapterLatencies;

  public static List<Double> getMetricLatencyMillisBuckets() {
    final int MAX_NUM_FINITE_BUCKETS = 50;
    final double BASE = 1.25;
    final double SCALE_FACTOR = 0.25;
    final double MAX_VALUE = 600;
    double bucketValue = SCALE_FACTOR;

    List<Double> rpcMillisBucketBoundaries = new ArrayList<Double>(MAX_NUM_FINITE_BUCKETS);
    for (int i = 0; i < MAX_NUM_FINITE_BUCKETS && bucketValue <= MAX_VALUE; i++) {
      rpcMillisBucketBoundaries.add(bucketValue);
      bucketValue *= BASE;
    }
    return rpcMillisBucketBoundaries;
  }

  public Metrics(OpenTelemetry openTelemetry) {
    Meter meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE);
    spannerClientLibLatencies =
        meter
            .histogramBuilder(SPANNER_CLIENT_LIB_LATENCY)
            .setDescription(SPANNER_CLIENT_LIB_LATENCY_DESCRIPTION)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(getMetricLatencyMillisBuckets())
            .build();
    pgadapterLatencies =
        meter
            .histogramBuilder(PGADAPTER_LATENCY)
            .setDescription(PGADAPTER_LATENCY_DESCRIPTION)
            .setUnit("ms")
            .setExplicitBucketBoundariesAdvice(getMetricLatencyMillisBuckets())
            .build();
  }

  @InternalApi
  public void recordClientLibLatency(long value, Attributes attributes) {
    spannerClientLibLatencies.record(value, attributes);
  }

  @InternalApi
  public void recordPGAdapterLatency(long value, Attributes attributes) {
    pgadapterLatencies.record(value, attributes);
  }
}
