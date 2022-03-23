// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter.benchmarks;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, warmups = 0)
@Measurement(batchSize = 5, iterations = 1, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(batchSize = 0, iterations = 0)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(Threads.MAX)
public class AbstractBenchmark {
  static {
    System.setProperty(
        "java.util.logging.config.file",
        ClassLoader.getSystemResource("logging.properties").getPath());
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            //            .include(AbstractBenchmark.class.getPackage().getName())
            //            .addProfiler(GCProfiler.class)
            .include("PgAdapterJdbcSimpleModeBenchmark.testSelect1")
            //            .include("ClientLibraryBenchmark.testSelect1")
            .forks(1)
            .jvmArgs("-ea")
            .build();

    new Runner(opt).run();
  }
}
