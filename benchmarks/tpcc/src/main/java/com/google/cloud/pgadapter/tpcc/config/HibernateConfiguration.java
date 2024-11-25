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
package com.google.cloud.pgadapter.tpcc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "hibernate")
public class HibernateConfiguration {

  private boolean showSql;

  private int batchSize;

  private int poolSize;

  private boolean orderInserts;

  private boolean orderUpdates;

  private boolean batchVersionedData;

  private boolean autoBatchDml;

  public boolean isAutoBatchDml() {
    return autoBatchDml;
  }

  public void setAutoBatchDml(boolean autoBatchDml) {
    this.autoBatchDml = autoBatchDml;
  }

  public boolean isBatchVersionedData() {
    return batchVersionedData;
  }

  public void setBatchVersionedData(boolean batchVersionedData) {
    this.batchVersionedData = batchVersionedData;
  }

  public boolean isOrderInserts() {
    return orderInserts;
  }

  public void setOrderInserts(boolean orderInserts) {
    this.orderInserts = orderInserts;
  }

  public boolean isOrderUpdates() {
    return orderUpdates;
  }

  public void setOrderUpdates(boolean orderUpdates) {
    this.orderUpdates = orderUpdates;
  }

  public boolean isShowSql() {
    return showSql;
  }

  public void setShowSql(boolean showSql) {
    this.showSql = showSql;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getPoolSize() {
    return poolSize;
  }

  public void setPoolSize(int poolSize) {
    this.poolSize = poolSize;
  }
}
