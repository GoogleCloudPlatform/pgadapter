package com.google.cloud.spanner.pgadapter.logging;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.LogManager;

public class DefaultLogConfiguration {

  public static void configureLogging() throws IOException {
    if (System.getProperty("java.util.logging.config.file") == null && System.getProperty("java.util.logging.config.class") == null) {
      // System.setProperty("java.util.logging.config.class", "com.google.cloud.spanner.pgadapter.logging.DefaultLogConfiguration");
      LogManager.getLogManager().readConfiguration(DefaultLogConfiguration.class.getClassLoader().getResourceAsStream("default-logging.properties"));
    }
  }

  private DefaultLogConfiguration() {}

}
