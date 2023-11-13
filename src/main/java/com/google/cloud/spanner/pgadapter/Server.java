// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.pgadapter;

import com.google.auth.Credentials;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.cloudtrace.v2.AttributeValue;
import com.google.devtools.cloudtrace.v2.TruncatableString;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

/** Effectively this is the main class */
public class Server {

  /**
   * Main method for running a Spanner PostgreSQL Adapter {@link Server} as a stand-alone
   * application. Here we call for parameter parsing and start the Proxy Server.
   */
  public static void main(String[] args) {
    try {
      OptionsMetadata optionsMetadata = extractMetadata(args, System.out);
      OpenTelemetry openTelemetry = setupOpenTelemetry(optionsMetadata);
      ProxyServer server = new ProxyServer(optionsMetadata, openTelemetry);
      server.startServer();
    } catch (Exception e) {
      printError(e, System.err, System.out);
    }
  }

  static OpenTelemetry setupOpenTelemetry(OptionsMetadata optionsMetadata) throws IOException {
    if (!optionsMetadata.isEnableOpenTelemetry()) {
      return OpenTelemetry.noop();
    }

    if (getOpenTelemetrySetting("otel.traces.exporter") == null) {
      System.setProperty("otel.traces.exporter", "none");
    }
    if (getOpenTelemetrySetting("otel.metrics.exporter") == null) {
      System.setProperty("otel.metrics.exporter", "none");
    }
    if (getOpenTelemetrySetting("otel.logs.exporter") == null) {
      System.setProperty("otel.logs.exporter", "none");
    }
    if (getOpenTelemetrySetting("otel.service.name") == null) {
      System.setProperty("otel.service.name", "pgadapter");
    }

    TraceConfiguration.Builder builder =
        TraceConfiguration.builder().setDeadline(Duration.ofMillis(30000));
    String projectId = optionsMetadata.getTelemetryProjectId();
    if (projectId != null) {
      builder.setProjectId(projectId);
    }
    Credentials credentials = optionsMetadata.getTelemetryCredentials();
    if (credentials != null) {
      builder.setCredentials(credentials);
    }
    builder.setFixedAttributes(
        ImmutableMap.of(
            "service.name",
            AttributeValue.newBuilder()
                .setStringValue(
                    TruncatableString.newBuilder()
                        .setValue(
                            Objects.requireNonNull(getOpenTelemetrySetting("otel.service.name")))
                        .build())
                .build()));
    TraceConfiguration configuration = builder.build();
    TraceExporter traceExporter = TraceExporter.createWithConfiguration(configuration);
    Sampler sampler;
    if (optionsMetadata.getOpenTelemetryTraceRatio() == null) {
      sampler = Sampler.parentBased(Sampler.alwaysOn());
    } else {
      sampler =
          Sampler.parentBased(
              Sampler.traceIdRatioBased(optionsMetadata.getOpenTelemetryTraceRatio()));
    }
    return AutoConfiguredOpenTelemetrySdk.builder()
        .addTracerProviderCustomizer(
            (sdkTracerProviderBuilder, configProperties) ->
                sdkTracerProviderBuilder
                    .setSampler(sampler)
                    .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build()))
        .build()
        .getOpenTelemetrySdk();
  }

  static String getOpenTelemetrySetting(String systemProperty) {
    if (System.getProperties().containsKey(systemProperty)) {
      return System.getProperty(systemProperty);
    }
    String envVar = convertOpenTelemetrySystemPropertyToEnvVar(systemProperty);
    if (System.getenv().containsKey(envVar)) {
      return System.getenv(envVar);
    }
    return null;
  }

  static String convertOpenTelemetrySystemPropertyToEnvVar(String systemProperty) {
    return systemProperty.replaceAll("\\.", "_").toUpperCase(Locale.ENGLISH);
  }

  static OptionsMetadata extractMetadata(String[] args, PrintStream out) {
    out.printf("-- Starting PGAdapter version %s --\n", getVersion());
    OptionsMetadata optionsMetadata = new OptionsMetadata(args);
    out.printf("-- PostgreSQL version: %s -- \n", optionsMetadata.getServerVersion());
    if (System.getProperty("javax.net.ssl.keyStore") != null) {
      if (!new File(System.getProperty("javax.net.ssl.keyStore")).exists()) {
        throw new IllegalArgumentException(
            "Key store " + System.getProperty("javax.net.ssl.keyStore") + " does not exist");
      }
    }

    return optionsMetadata;
  }

  static void printError(Exception exception, PrintStream err, PrintStream out) {
    err.printf(
        "The server could not be started because an error occurred: %s\n",
        (exception.getMessage() == null ? exception.toString() : exception.getMessage()));
    out.print("Run with option -h or --help to get help\n");
    out.printf("Version: %s\n", getVersion());
  }

  public static String getVersion() {
    String version = Server.class.getPackage().getImplementationVersion();
    if (version != null) {
      return version;
    }

    try {
      MavenXpp3Reader reader = new MavenXpp3Reader();
      Model model = reader.read(new FileReader("pom.xml"));
      if (model.getVersion() != null) {
        return model.getVersion();
      }
    } catch (Exception e) {
      // ignore
    }

    return "(unknown)";
  }
}
