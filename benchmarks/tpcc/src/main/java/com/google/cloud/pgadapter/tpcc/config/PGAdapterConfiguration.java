package com.google.cloud.pgadapter.tpcc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "pgadapter")
public class PGAdapterConfiguration {
  private boolean inProcess;

  private int numChannels;

  private String credentials;

  private boolean enableOpenTelemetry;

  private boolean enableOpenTelemetryMetrics;

  private double openTelemetrySampleRate;

  private boolean disableInternalRetries;

  private String host;

  private int port;

  private String connectionUrl;

  public boolean isInProcess() {
    return inProcess;
  }

  public void setInProcess(boolean inProcess) {
    this.inProcess = inProcess;
  }

  public int getNumChannels() {
    return numChannels;
  }

  public void setNumChannels(int numChannels) {
    this.numChannels = numChannels;
  }

  public boolean isEnableOpenTelemetry() {
    return enableOpenTelemetry;
  }

  public void setEnableOpenTelemetry(boolean enableOpenTelemetry) {
    this.enableOpenTelemetry = enableOpenTelemetry;
  }

  public boolean isEnableOpenTelemetryMetrics() {
    return enableOpenTelemetryMetrics;
  }

  public void setEnableOpenTelemetryMetrics(boolean enableOpenTelemetryMetrics) {
    this.enableOpenTelemetryMetrics = enableOpenTelemetryMetrics;
  }

  public double getOpenTelemetrySampleRate() {
    return openTelemetrySampleRate;
  }

  public void setOpenTelemetrySampleRate(double openTelemetrySampleRate) {
    this.openTelemetrySampleRate = openTelemetrySampleRate;
  }

  public boolean isDisableInternalRetries() {
    return disableInternalRetries;
  }

  public void setDisableInternalRetries(boolean disableInternalRetries) {
    this.disableInternalRetries = disableInternalRetries;
  }

  public String getCredentials() {
    return credentials;
  }

  public void setCredentials(String credentials) {
    this.credentials = credentials;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getConnectionUrl() {
    return connectionUrl;
  }

  public void setConnectionUrl(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }
}
