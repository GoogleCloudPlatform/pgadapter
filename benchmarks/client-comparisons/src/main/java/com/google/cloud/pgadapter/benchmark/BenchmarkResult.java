package com.google.cloud.pgadapter.benchmark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class BenchmarkResult {

  final String name;
  final int parallelism;
  final Duration avg;
  final Duration p50;
  final Duration p90;
  final Duration p95;
  final Duration p99;

  BenchmarkResult(String name, int parallelism, ConcurrentLinkedQueue<Duration> durations) {
    this.name = name;
    this.parallelism = parallelism;
    List<Duration> list = new ArrayList<>(durations);
    list.sort(Duration::compareTo);
    avg = list.stream().reduce(Duration::plus).orElse(Duration.ZERO).dividedBy(list.size());
    p50 = list.get(durations.size() / 2);
    p90 = list.get(durations.size() * 90 / 100);
    p95 = list.get(durations.size() * 95 / 100);
    p99 = list.get(durations.size() * 99 / 100);
  }

  @Override
  public String toString() {
    return String.format(
        "name: %s\nparallelism: %d\navg: %s\np50: %s\np90: %s\np95: %s\np99: %s\n\n",
        name, parallelism, avg, p50, p90, p95, p99);
  }
}
