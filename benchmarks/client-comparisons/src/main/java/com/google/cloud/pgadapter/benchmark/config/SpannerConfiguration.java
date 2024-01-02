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
}
