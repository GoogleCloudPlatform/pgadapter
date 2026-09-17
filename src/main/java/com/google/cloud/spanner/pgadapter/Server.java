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
import java.util.concurrent.atomic.AtomicBoolean;
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

  /** Exit code that is used when PGAdapter itself could not be started. */
  private static final int STARTUP_FAILED_EXIT_CODE = 1;

  /**
   * Exit code that is used when the tool given with --command could not be executed. 127 is the
   * conventional shell exit code for 'command not found'.
   */
  private static final int COMMAND_NOT_EXECUTED_EXIT_CODE = 127;

  /** The client tool that was started by {@link #runCommand}, if any. */
  private static final AtomicReference<Process> clientProcess = new AtomicReference<>();

  /**
   * Serializes starting the client tool against shutting down, so that a tool that is still being
   * started cannot be missed by the shutdown hook and left running.
   */
  @VisibleForTesting static final Object clientProcessLock = new Object();

  /** Set by the shutdown hook, so that a client tool that is still starting is not started. */
  @VisibleForTesting static final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  private static volatile ShutdownHandler shutdownHandler;

  /**
   * Main method for running a Spanner PostgreSQL Adapter {@link Server} as a stand-alone
   * application. Here we call for parameter parsing and start the Proxy Server.
   */
  public static void main(String[] args) {
    int exitCode = run(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  /**
   * Runs PGAdapter and returns the exit code that this process should use. Returns zero without
   * blocking when PGAdapter is started in the background, as the proxy keeps the JVM alive.
   */
  @VisibleForTesting
  static int run(String[] args) {
    try {
      DefaultLogConfiguration.configureLogging(args);
      OptionsMetadata optionsMetadata = extractMetadata(args, System.out);
      OpenTelemetry openTelemetry = setupOpenTelemetry(optionsMetadata);
      ProxyServer proxyServer = new ProxyServer(optionsMetadata, openTelemetry);
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
        return runCommandAndStop(proxyServer, database, optionsMetadata.getCommand());
      }
      // There's no command that should be executed against PGAdapter, so we should keep it
      // running in the background. Create a shutdown handler and register signal handlers for the
      // signals that should terminate the server.
      Server.shutdownHandler = proxyServer.getOrCreateShutdownHandler();
      registerSignalHandlers();
      return 0;
    } catch (Exception e) {
      printError(e, System.err, System.out);
      return STARTUP_FAILED_EXIT_CODE;
    }
  }

  /**
   * Runs the given client tool and stops PGAdapter when it has finished, returning the exit code of
   * the tool.
   *
   * <p>No handler is registered for TERM: the JVM's own handler already runs the shutdown hook
   * below and then exits with 143, and registering a handler here would take that away from
   * applications that embed PGAdapter.
   */
  private static int runCommandAndStop(
      ProxyServer proxyServer, @Nullable String database, String... command) {
    ignoreInterruptSignal();
    Thread shutdownHook = createShutdownHook(proxyServer, "pgadapter-shutdown-handler");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    try {
      // runCommand will block until the command has finished (e.g. when the user exits psql).
      return runCommand(proxyServer, database, command);
    } catch (IOException couldNotStart) {
      System.err.printf(
          "The command could not be started: %s%n",
          couldNotStart.getMessage() == null
              ? couldNotStart.toString()
              : couldNotStart.getMessage());
      return COMMAND_NOT_EXECUTED_EXIT_CODE;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return COMMAND_NOT_EXECUTED_EXIT_CODE;
    } finally {
      // Shut down PGAdapter when the command has finished. This must also happen when the
      // command could not be started, as the proxy holds non-daemon threads.
      stopProxyServer(proxyServer);
      removeShutdownHook(shutdownHook);
    }
  }

  @VisibleForTesting
  public static int runCommand(
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
    return startAndWait(builder);
  }

  /** Starts the given process and waits for it, tracking it so that it can be stopped on exit. */
  static int startAndWait(ProcessBuilder builder) throws IOException, InterruptedException {
    Process process;
    synchronized (clientProcessLock) {
      if (shuttingDown.get()) {
        // Shutdown already looked for a client tool and found none, so starting one now would
        // leave it running after this process has gone.
        return COMMAND_NOT_EXECUTED_EXIT_CODE;
      }
      process = builder.start();
      clientProcess.set(process);
    }
    try {
      return process.waitFor();
    } finally {
      clientProcess.compareAndSet(process, null);
    }
  }

  /**
   * Stops the client tool that was started by this process. This is a no-op if no client tool is
   * running. Killing this process does not kill the client tool, so it must be stopped explicitly.
   */
  static void destroyClientProcess() {
    Process process = clientProcess.getAndSet(null);
    if (process == null || !process.isAlive()) {
      return;
    }
    process.destroy();
    try {
      if (!process.waitFor(5L, TimeUnit.SECONDS)) {
        process.destroyForcibly();
      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      process.destroyForcibly();
    }
  }

  /** Creates a hook that stops the client tool and then the given server. */
  static Thread createShutdownHook(ProxyServer proxyServer, String name) {
    // Static state, so reset it in case this process has already run a command.
    shuttingDown.set(false);
    return new Thread(
        () -> {
          // The JVM halts as soon as this thread returns, so it must not return while a client
          // tool is still being started on another thread. Holding the lock makes it wait.
          synchronized (clientProcessLock) {
            shuttingDown.set(true);
            destroyClientProcess();
          }
          stopProxyServer(proxyServer);
        },
        name);
  }

  static void removeShutdownHook(@Nullable Thread shutdownHook) {
    if (shutdownHook == null) {
      return;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException ignore) {
      // The JVM is already shutting down and the hook is running (or has run). Ignore.
    }
  }

  /** Stops the given {@link ProxyServer}. This method is safe to call more than once. */
  static void stopProxyServer(@Nullable ProxyServer proxyServer) {
    if (proxyServer == null) {
      return;
    }
    try {
      // stopServer() is idempotent and blocks until the server has terminated. Concurrent callers
      // must therefore wait here rather than return early: a shutdown hook that returns while
      // another thread is still stopping the server lets the JVM halt halfway through.
      proxyServer.stopServer();
    } catch (Throwable ignore) {
      // Ignore any errors during shutdown, as there is nothing that we can do about them, and
      // printing them would only pollute the output of the client tool.
    }
  }

  /**
   * Registers signal handlers for TERM, INT, and QUIT. This is only done when PGAdapter owns the
   * process, as replacing a signal handler affects the entire JVM.
   */
  static void registerSignalHandlers() {
    registerSignalHandler("TERM", "handleTerm", Level.WARNING);
    registerSignalHandler("INT", "handleInt", Level.WARNING);
    // QUIT is normally already registered by the JVM, so this is expected to fail. QUIT signals
    // then just stop the server through the JVM instead of through PGAdapter.
    registerSignalHandler("QUIT", "handleQuit", Level.FINE);
  }

  /**
   * Stops Ctrl+C from killing PGAdapter while it is wrapping a client tool. The terminal sends INT
   * to the whole foreground process group, so the tool itself decides what to do with it (psql
   * cancels the running query rather than exiting).
   *
   * <p>A handler that does nothing is used rather than SIG_IGN, because SIG_IGN would be inherited
   * by the client tool across exec, while handlers are reset to the default.
   */
  static void ignoreInterruptSignal() {
    registerSignalHandler("INT", "handleIgnore", Level.WARNING);
  }

  /**
   * Registers a single signal handler. This method uses reflection and fails gracefully if signal
   * handling is not available on this JVM, or if the signal is already owned by the JVM.
   */
  private static void registerSignalHandler(String signalName, String methodName, Level failLevel) {
    Class<?> signalClass = getSignalClass();
    Class<?> signalHandlerClass = getSignalHandlerClass();
    if (signalClass == null || signalHandlerClass == null) {
      return;
    }
    try {
      Method handleMethod =
          signalClass.getDeclaredMethod("handle", signalClass, signalHandlerClass);
      Constructor<?> signalConstructor = signalClass.getConstructor(String.class);
      Object signal = signalConstructor.newInstance(signalName);
      Object handler = createSignalHandler(signalClass, signalHandlerClass, methodName);
      handleMethod.invoke(null, signal, handler);
    } catch (Throwable throwable) {
      logger.log(failLevel, "Failed to register signal handler for " + signalName, throwable);
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
    logger.log(Level.INFO, "Server received TERM");
    if (Server.shutdownHandler == null) {
      return;
    }
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

  /** Signal handler that does nothing. See {@link #ignoreInterruptSignal()}. */
  static void handleIgnore(Object ignoredSignal) {}

  /**
   * This method is called by the signal handler that is registered for QUIT. Normally unused, as
   * the JVM already registers QUIT.
   */
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
