package com.google.cloud.spanner.pgadapter.sample;

import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * This class is added as a listener to the Spring Boot application and starts PGAdapter before any
 * DataSource is created.
 */
class PGAdapterInitializer implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {
  private PGAdapter pgAdapter;

  PGAdapter getPGAdapter() {
    return this.pgAdapter;
  }

  @Override
  public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
    ConfigurableEnvironment environment = event.getEnvironment();
    boolean useEmulator =
        Boolean.TRUE.equals(environment.getProperty("spanner.use_emulator", Boolean.class));
    this.pgAdapter = new PGAdapter(useEmulator);
  }
}
