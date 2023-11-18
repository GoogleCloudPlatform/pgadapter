package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.logging.Logger;

public class Main {
  private static final Logger log = Logger.getLogger(BenchmarkRunner.class.getName());

  public static void main(String[] args) throws Exception {
    OptionsMetadata options = OptionsMetadata.newBuilder()
        .setProject("appdev-soda-spanner-staging")
        .setInstance("knut-test-ycsb")
        .setDatabase("tpcc")
        .disableUnixDomainSockets()
        .setCredentialsFile("/home/loite/appdev-soda-spanner-staging.json")
        .setPort(9876)
        .build();
    ProxyServer server = new ProxyServer(options);
    server.startServer();
    server.awaitRunning();
    
    try {
      BenchmarkRunner runner = new BenchmarkRunner("jdbc:postgresql://localhost:9876/knut-test-db?preferQueryMode=simple");
//      runner.createSchema();
//      runner.prepareStatements();
      runner.loadWarehouse();
    } finally {
      server.stopServer();
    }
  }

}
