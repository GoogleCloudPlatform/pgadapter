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
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.pgadapter.ProxyServer.ShutdownMode;
import com.google.cloud.spanner.pgadapter.logging.DefaultLogConfiguration;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.cloudtrace.v2.AttributeValue;
import com.google.devtools.cloudtrace.v2.TruncatableString;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Effectively this is the main class */
public class Server {
  private static final Logger logger = Logger.getLogger(Server.class.getName());

  private static volatile ShutdownHandler shutdownHandler;
  private static boolean loggedMissingSignalSupport;

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
      if (!optionsMetadata.hasCommand()) {
        // There's no command that should be executed against PGAdapter, so we should keep it
        // running in the background. Create a shutdown handler and register signal handlers for the
        // signals that should terminate the server before starting the server.
        Server.shutdownHandler = proxyServer.getOrCreateShutdownHandler();
        registerSignalHandlers();
      }
      proxyServer.startServer();

      if (optionsMetadata.hasCommand()) {
        // A command (tool) has been specified. This should be started and connected to PGAdapter.
        // This will take over stdin and stdout, so we disable PGAdapter logging to prevent it from
        // polluting standard output.
        DefaultLogConfiguration.disableLogging();
        String database = null;
        if (optionsMetadata.getDefaultDatabaseId() != null) {
          database = optionsMetadata.getDefaultDatabaseId().getDatabase();
        }
        Object previousIntHandler = ignoreInterruptSignal();
        try {
          // runCommand will block until the command has finished (e.g. when the user exits psql).
          runCommand(proxyServer, database, optionsMetadata.getCommand());
        } finally {
          restoreSignalHandler("INT", previousIntHandler);
          // Shut down PGAdapter when the command has finished.
          stopServerQuietly(proxyServer);
        }
      }
    } catch (Exception e) {
      printError(e, System.err, System.out);
    }
  }

  @VisibleForTesting
  public static void runCommand(
      ProxyServer proxyServer, @Nullable String database, String... command)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(command);
    builder.environment().put("PGHOST", "localhost");
    builder.environment().put("PGPORT", String.valueOf(proxyServer.getLocalPort()));
    if (database != null) {
      builder.environment().put("PGDATABASE", database);
    }
    builder.inheritIO();
    AtomicReference<Process> processRef = new AtomicReference<>();
    Thread shutdownHook =
        new Thread(() -> stopCommand(processRef.get(), proxyServer), "pgadapter-cmd-shutdown-hook");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    try {
      Process process = builder.start();
      processRef.set(process);
      process.waitFor();
    } finally {
      removeShutdownHook(shutdownHook);
    }
  }

  @VisibleForTesting
  static void stopCommand(@Nullable Process process, ProxyServer proxyServer) {
    if (process != null) {
      process.destroy();
      try {
        if (!process.waitFor(5L, TimeUnit.SECONDS)) {
          process.destroyForcibly();
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
    }
    // stopServer() defaults to ShutdownMode.FAST, which actively closes open client connections
    // rather than waiting indefinitely for active sessions to finish.
    stopServerQuietly(proxyServer);
  }

  private static void stopServerQuietly(@Nullable ProxyServer proxyServer) {
    if (proxyServer == null) {
      return;
    }
    try {
      proxyServer.stopServer();
    } catch (Exception exception) {
      logger.log(Level.FINE, "Error while stopping server", exception);
    }
  }

  private static void removeShutdownHook(Thread shutdownHook) {
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException ignore) {
      // The JVM is already shutting down and executing the hook.
    }
  }

  /**
   * Registers signal handlers for TERM, INT, and QUIT. This method uses reflection and fails
   * gracefully if signal handling is not available on this JVM.
   *
   * <p>MUST NOT be called from a static initializer: loading {@link Server} must not mutate the
   * host JVM's signal handlers, because {@link ProxyServer} can be embedded in another application
   * (e.g. {@code new ProxyServer(options)} invokes {@link #setupOpenTelemetry(OptionsMetadata)}).
   */
  static void registerSignalHandlers() {
    registerSignalHandler("TERM", "handleTerm", Level.WARNING);
    registerSignalHandler("INT", "handleInt", Level.WARNING);
    // Log this at FINE level, as QUIT is normally already registered by the JVM. This means that
    // registering QUIT will fail on most JVMs, and QUIT signals are handled directly by the JVM.
    registerSignalHandler("QUIT", "handleQuit", Level.FINE);
  }

  /**
   * Prevents Ctrl+C (SIGINT) from stopping PGAdapter while wrapping a client tool (-cmd /
   * --command). The terminal sends INT to the entire foreground process group, so the client tool
   * (e.g. psql) handles INT itself to cancel active queries without terminating the connection.
   *
   * <p>A Java no-op handler ({@link #handleIgnore}) is used instead of {@code SIG_IGN} because
   * {@code SIG_IGN} dispositions are inherited across {@code fork}/{@code exec}, which would cause
   * the child process (psql) to ignore Ctrl+C as well. A custom handler resets to {@code SIG_DFL}
   * upon {@code exec}.
   */
  @Nullable
  static Object ignoreInterruptSignal() {
    return registerSignalHandler("INT", "handleIgnore", Level.WARNING);
  }

  @Nullable
  static Object registerSignalHandler(
      String signalName, String methodName, Level registrationFailLevel) {
    Class<?> signalClass = getSignalClass();
    Class<?> signalHandlerClass = getSignalHandlerClass();
    if (signalClass == null || signalHandlerClass == null) {
      return null;
    }
    try {
      Object handler = createSignalHandler(signalClass, signalHandlerClass, methodName);
      return setSignalHandler(
          signalClass, signalHandlerClass, signalName, handler, registrationFailLevel);
    } catch (Throwable exception) {
      logger.log(Level.WARNING, "Failed to register signal handlers", exception);
      return null;
    }
  }

  @Nullable
  static Object restoreSignalHandler(String signalName, @Nullable Object previousHandler) {
    if (previousHandler == null) {
      return null;
    }
    Class<?> signalClass = getSignalClass();
    Class<?> signalHandlerClass = getSignalHandlerClass();
    if (signalClass == null || signalHandlerClass == null) {
      return null;
    }
    return setSignalHandler(
        signalClass, signalHandlerClass, signalName, previousHandler, Level.FINE);
  }

  @Nullable
  private static Object setSignalHandler(
      Class<?> signalClass,
      Class<?> signalHandlerClass,
      String signalName,
      Object handler,
      Level failLevel) {
    try {
      Method handleMethod =
          signalClass.getDeclaredMethod("handle", signalClass, signalHandlerClass);
      Constructor<?> signalConstructor = signalClass.getConstructor(String.class);
      Object signal = signalConstructor.newInstance(signalName);
      return handleMethod.invoke(null, signal, handler);
    } catch (Throwable throwable) {
      logger.log(failLevel, "Failed to register signal handler for " + signalName, throwable);
      return null;
    }
  }

  private static Class<?> getSignalClass() {
    try {
      return Class.forName("sun.misc.Signal");
    } catch (ClassNotFoundException exception) {
      logMissingSignalSupport("sun.misc.Signal");
      return null;
    }
  }

  private static Class<?> getSignalHandlerClass() {
    try {
      return Class.forName("sun.misc.SignalHandler");
    } catch (ClassNotFoundException exception) {
      logMissingSignalSupport("sun.misc.SignalHandler");
      return null;
    }
  }

  private static synchronized void logMissingSignalSupport(String className) {
    if (!loggedMissingSignalSupport) {
      loggedMissingSignalSupport = true;
      logger.log(
          Level.INFO,
          "Cannot register shutdown signal handlers as "
              + className
              + " is not available on this JVM");
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

  /** Signal handler that does nothing. Used for INT in -cmd / --command mode. */
  static void handleIgnore(Object ignoredSignal) {}

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
        SpannerOptions.enableOpenTelemetryTraces();
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
                "service.name",
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
        SpannerOptions.enableOpenTelemetryMetrics();
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
                    .addResource(
                        Resource.create(
                            Attributes.of(AttributeKey.stringKey("service.name"), serviceName)))
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
      File pomFile = new File("pom.xml");
      if (pomFile.exists()) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(pomFile);
        NodeList list = doc.getElementsByTagName("version");
        for (int i = 0; i < list.getLength(); i++) {
          Node node = list.item(i);
          if (node.getParentNode().getNodeName().equals("project")) {
            return node.getTextContent();
          }
        }
      }
    } catch (Exception e) {
      // ignore
    }

    return null;
  }
}
