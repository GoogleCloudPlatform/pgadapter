// Copyright 2024 Google LLC
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
