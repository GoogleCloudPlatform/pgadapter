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

import static io.opentelemetry.semconv.ServiceAttributes.SERVICE_NAME;

import com.google.auth.Credentials;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.spanner.pgadapter.ProxyServer.ShutdownMode;
import com.google.cloud.spanner.pgadapter.logging.DefaultLogConfiguration;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.cloudtrace.v2.AttributeValue;
import com.google.devtools.cloudtrace.v2.TruncatableString;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

// import sun.misc.Signal;

/** Effectively this is the main class */
public class Server {
  private static final Logger logger = Logger.getLogger(Server.class.getName());

  private static ShutdownHandler shutdownHandler;

  /**
   * Main method for running a Spanner PostgreSQL Adapter {@link Server} as a stand-alone
   * application. Here we call for parameter parsing and start the Proxy Server.
   */
  public static void main(String[] args) {
    try {
      DefaultLogConfiguration.configureLogging(args);
      OptionsMetadata optionsMetadata = extractMetadata(args, System.out);
      OpenTelemetry openTelemetry = setupOpenTelemetry(optionsMetadata);
      ProxyServer proxyServer = new ProxyServer(optionsMetadata, openTelemetry);
      proxyServer.startServer();

      // Create a shutdown handler and register signal handlers for the signals that should
      // terminate the server.
      Server.shutdownHandler = ShutdownHandler.createForServer(proxyServer);
      registerSignalHandlers();
    } catch (Exception e) {
      printError(e, System.err, System.out);
    }
  }

  /**
   * Registers signal handlers for TERM, INT, and QUIT. This method uses reflection and fails
   * gracefully if signal handling is not available on this JVM.
   */
  static void registerSignalHandlers() {
    Class<?> signalClass = getSignalClass();
    Class<?> signalHandlerClass = getSignalHandlerClass();
    if (signalClass == null || signalHandlerClass == null) {
      return;
    }
    try {
      Method handleMethod =
          signalClass.getDeclaredMethod("handle", signalClass, signalHandlerClass);
      Constructor<?> signalConstructor = signalClass.getConstructor(String.class);

      Object termSignal = signalConstructor.newInstance("TERM");
      Object intSignal = signalConstructor.newInstance("INT");
      Object quitSignal = signalConstructor.newInstance("QUIT");

      Object termHandler = createSignalHandler(signalClass, signalHandlerClass, "handleTerm");
      Object intHandler = createSignalHandler(signalClass, signalHandlerClass, "handleInt");
      Object quitHandler = createSignalHandler(signalClass, signalHandlerClass, "handleQuit");

      // Register the actual signal handlers. This will fail if the JVM has already registered a
      // signal handler for the given signal. Normally, TERM and INT can be registered. QUIT is
      // normally registered by the JVM.
      try {
        handleMethod.invoke(null, termSignal, termHandler);
      } catch (Throwable throwable) {
        logger.log(Level.WARNING, "Failed to register signal handler for TERM", throwable);
      }
      try {
        handleMethod.invoke(null, intSignal, intHandler);
      } catch (Throwable throwable) {
        logger.log(Level.WARNING, "Failed to register signal handler for INT", throwable);
      }
      try {
        handleMethod.invoke(null, quitSignal, quitHandler);
      } catch (Throwable throwable) {
        // Log this at FINE level, as QUIT is normally already registered by the JVM. This means
        // that registering QUIT will fail on most JVMs. This means that QUIT signals will still
        // lead to the server stopping quickly, just that it is handled directly by the JVM instead
        // of PGAdapter.
        logger.log(Level.FINE, "Failed to register signal handler for QUIT", throwable);
      }
    } catch (Throwable exception) {
      logger.log(Level.WARNING, "Failed to register signal handlers", exception);
    }
  }

  private static Class<?> getSignalClass() {
    try {
      return Class.forName("sun.misc.Signal");
    } catch (ClassNotFoundException exception) {
      logger.log(
          Level.INFO,
          "Cannot register shutdown signal handlers as sun.misc.Signal is not available on this JVM");
      return null;
    }
  }

  private static Class<?> getSignalHandlerClass() {
    try {
      return Class.forName("sun.misc.SignalHandler");
    } catch (ClassNotFoundException exception) {
      logger.log(
          Level.INFO,
          "Cannot register shutdown signal handlers as sun.misc.SignalHandler is not available on this JVM");
      return null;
    }
  }

  /**
   * This method dynamically creates the signal handler lambdas that call the handleTerm, handleInt,
   * and handleQuit methods. We need to do this through reflection, because we cannot use the
   * sun.misc.SignalHandler interface directly to create the lambda expressions.
   */
  @IgnoreJRERequirement
  private static Object createSignalHandler(
      Class<?> signalClass, Class<?> signalHandlerClass, String handleMethodName) throws Throwable {
    MethodHandles.Lookup caller = MethodHandles.lookup();
    MethodType signalHandlerHandleMethodType = MethodType.methodType(void.class, signalClass);
    MethodType invokedType = MethodType.methodType(signalHandlerClass);

    CallSite handleTermSite =
        LambdaMetafactory.metafactory(
            caller,
            "handle",
            invokedType,
            signalHandlerHandleMethodType,
            caller.findStatic(
                Server.class, handleMethodName, MethodType.methodType(void.class, Object.class)),
            signalHandlerHandleMethodType);
    MethodHandle factory = handleTermSite.getTarget();
    return factory.invoke();
  }

  /** This method is called by the signal handler that is registered for TERM. */
  static void handleTerm(Object ignoredSignal) {
    if (Server.shutdownHandler == null) {
      return;
    }
    logger.log(Level.INFO, "Server received TERM");
    Server.shutdownHandler.shutdown(ShutdownMode.SMART);
  }

  /** This method is called by the signal handler that is registered for INT. */
  static void handleInt(Object ignoredSignal) {
    if (Server.shutdownHandler == null) {
      return;
    }
    logger.log(Level.INFO, "Server received INT");
    Server.shutdownHandler.shutdown(ShutdownMode.FAST);
  }

  /** This method is called by the signal handler that is registered for QUIT. */
  static void handleQuit(Object ignoredSignal) {
    if (Server.shutdownHandler == null) {
      return;
    }
    logger.log(Level.INFO, "Server received QUIT");
    Server.shutdownHandler.shutdown(ShutdownMode.IMMEDIATE);
  }

  /** Creates an {@link OpenTelemetry} object from the given options. */
  static OpenTelemetry setupOpenTelemetry(OptionsMetadata optionsMetadata) {
    if (!optionsMetadata.isEnableOpenTelemetry()
        && !optionsMetadata.isEnableOpenTelemetryMetrics()) {
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
      System.setProperty("otel.service.name", "pgadapter-" + ThreadLocalRandom.current().nextInt());
    }
    String serviceName = Objects.requireNonNull(getOpenTelemetrySetting("otel.service.name"));

    try {
      String projectId = optionsMetadata.getTelemetryProjectId();
      Credentials credentials = optionsMetadata.getTelemetryCredentials();
      AutoConfiguredOpenTelemetrySdkBuilder openTelemetryBuilder =
          AutoConfiguredOpenTelemetrySdk.builder();
      if (optionsMetadata.isEnableOpenTelemetry()) {
        TraceConfiguration.Builder builder =
            TraceConfiguration.builder().setDeadline(Duration.ofSeconds(60L));
        if (projectId != null) {
          builder.setProjectId(projectId);
        }
        if (credentials != null) {
          builder.setCredentials(credentials);
        }
        builder.setFixedAttributes(
            ImmutableMap.of(
                SERVICE_NAME.getKey(),
                AttributeValue.newBuilder()
                    .setStringValue(TruncatableString.newBuilder().setValue(serviceName).build())
                    .build()));
        TraceConfiguration configuration = builder.build();
        SpanExporter traceExporter = TraceExporter.createWithConfiguration(configuration);
        Sampler sampler;
        if (optionsMetadata.getOpenTelemetryTraceRatio() == null) {
          sampler = Sampler.parentBased(Sampler.traceIdRatioBased(0.05d));
        } else {
          sampler =
              Sampler.parentBased(
                  Sampler.traceIdRatioBased(optionsMetadata.getOpenTelemetryTraceRatio()));
        }
        openTelemetryBuilder.addTracerProviderCustomizer(
            (sdkTracerProviderBuilder, configProperties) ->
                sdkTracerProviderBuilder
                    .setSampler(sampler)
                    .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build()));
      }
      if (optionsMetadata.isEnableOpenTelemetryMetrics()) {
        MetricExporter cloudMonitoringExporter =
            GoogleCloudMetricExporter.createWithConfiguration(
                MetricConfiguration.builder()
                    // Configure the cloud project id.
                    .setProjectId(projectId)
                    // Set the credentials to use when writing to the Cloud Monitoring API
                    .setCredentials(credentials)
                    .build());
        openTelemetryBuilder.addMeterProviderCustomizer(
            (sdkMeterProviderBuilder, configProperties) ->
                sdkMeterProviderBuilder
                    .addResource(Resource.create(Attributes.of(SERVICE_NAME, serviceName)))
                    .registerMetricReader(
                        PeriodicMetricReader.builder(cloudMonitoringExporter).build()));
      }
      return openTelemetryBuilder.build().getOpenTelemetrySdk();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
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
