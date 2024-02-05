package com.google.cloud.pgadapter.benchmark.config;

import com.google.cloud.spanner.DatabaseId;
import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "spanner")
public class SpannerConfiguration {
  private String project;
  private String instance;
  private String database;

  private boolean useVirtualThreads;
  private long randomizePositionTransactionsPerSecondThreshold;
  private boolean optimizeSessionPoolFuture;
  private boolean optimizeUnbalancedCheck;
  private boolean useStickySessionClient;
  private boolean useStreamingSql;

  private final Supplier<DatabaseId> databaseIdSupplier =
      Suppliers.memoize(() -> DatabaseId.of(project, instance, database));

  public DatabaseId getDatabaseId() {
    return databaseIdSupplier.get();
  }

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getInstance() {
    return instance;
  }

  public void setInstance(String instance) {
    this.instance = instance;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public boolean isUseVirtualThreads() {
    return useVirtualThreads;
  }

  public void setUseVirtualThreads(boolean useVirtualThreads) {
    this.useVirtualThreads = useVirtualThreads;
  }

  public long getRandomizePositionTransactionsPerSecondThreshold() {
    return randomizePositionTransactionsPerSecondThreshold;
  }

  public void setRandomizePositionTransactionsPerSecondThreshold(
      long randomizePositionTransactionsPerSecondThreshold) {
    this.randomizePositionTransactionsPerSecondThreshold =
        randomizePositionTransactionsPerSecondThreshold;
  }

  public boolean isOptimizeSessionPoolFuture() {
    return optimizeSessionPoolFuture;
  }

  public void setOptimizeSessionPoolFuture(boolean optimizeSessionPoolFuture) {
    this.optimizeSessionPoolFuture = optimizeSessionPoolFuture;
  }

  public boolean isOptimizeUnbalancedCheck() {
    return optimizeUnbalancedCheck;
  }

  public void setOptimizeUnbalancedCheck(boolean optimizeUnbalancedCheck) {
    this.optimizeUnbalancedCheck = optimizeUnbalancedCheck;
  }

  public Supplier<DatabaseId> getDatabaseIdSupplier() {
    return databaseIdSupplier;
  }

  public boolean isUseStickySessionClient() {
    return useStickySessionClient;
  }

  public void setUseStickySessionClient(boolean useStickySessionClient) {
    this.useStickySessionClient = useStickySessionClient;
  }

  public boolean isUseStreamingSql() {
    return useStreamingSql;
  }

  public void setUseStreamingSql(boolean useStreamingSql) {
    this.useStreamingSql = useStreamingSql;
  }
}
