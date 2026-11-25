// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.connection;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.cloud.spanner.connection.ConnectionOptions.Builder;
import com.google.cloud.spanner.connection.ConnectionOptions.ParsedUriProperties;
import com.google.cloud.spanner.connection.ConnectionOptions.StartupPropertyPolicy;
import com.google.cloud.spanner.connection.StatementExecutor.StatementExecutorType;
import com.google.cloud.spanner.pgadapter.logging.GrpcLogInterceptor;
import com.google.common.collect.ImmutableList;
import java.util.logging.Level;

/** Simple helper class to get access to package-private methods in ConnectionOptions. */
@InternalApi
public class PGAdapterConnectionOptionsHelper {
  /** Private constructor to prevent instantiation. */
  private PGAdapterConnectionOptionsHelper() {}

  // TODO: Remove when Builder.setCredentials(..) has been made public.
  public static Builder setCredentials(Builder connectionOptionsBuilder, Credentials credentials) {
    return connectionOptionsBuilder.setCredentials(credentials);
  }

  public static Builder useDirectExecutor(Builder connectionOptionsBuilder) {
    return connectionOptionsBuilder.setStatementExecutorType(StatementExecutorType.DIRECT_EXECUTOR);
  }

  /** Adds a gRPC log interceptor if the log level has been set to FINEST. */
  public static Builder maybeAddGrpcLogInterceptor(
      Builder connectionOptionsBuilder, boolean logGrpcMessagesAsInfo) {
    if (GrpcLogInterceptor.isGrpcFinestLogEnabled()) {
      return connectionOptionsBuilder.setConfigurator(
          options ->
              options.setInterceptorProvider(
                  () -> ImmutableList.of(new GrpcLogInterceptor(Level.FINEST))));
    }
    if (logGrpcMessagesAsInfo) {
      return connectionOptionsBuilder.setConfigurator(
          options ->
              options.setInterceptorProvider(
                  () -> ImmutableList.of(new GrpcLogInterceptor(Level.INFO))));
    }
    return connectionOptionsBuilder;
  }

  /**
   * Validates whether the given value can be converted to a valid value for the specified property.
   */
  public static boolean isValidPropertyValue(ConnectionProperty<?> property, String value) {
    try {
      return property.convert(value) != null;
    } catch (Exception exception) {
      return false;
    }
  }

  /**
   * Parses and validates all connection properties in {@code serverUri} and applies them
   * programmatically on {@code connectionOptionsBuilder}, so they are not subject to {@link
   * StartupPropertyPolicy#REJECT} when the URI is later set on the builder.
   */
  public static Builder addPropertiesFromServerUri(
      Builder connectionOptionsBuilder, String serverUri) {
    ParsedUriProperties parsedProperties = ConnectionOptions.parseUriProperties(serverUri);
    String lenientValue = parsedProperties.getValue(ConnectionProperties.LENIENT.getKey());
    ConnectionPropertyValue<Boolean> lenientPropertyValue =
        lenientValue == null ? null : ConnectionProperties.LENIENT.convert(lenientValue);
    boolean lenient =
        lenientPropertyValue != null && Boolean.TRUE.equals(lenientPropertyValue.getValue());
    ConnectionOptions.checkValidProperties(lenient, parsedProperties, StartupPropertyPolicy.ALLOW);
    for (ConnectionPropertyValue<?> propertyValue :
        ConnectionProperties.parseValues(parsedProperties).values()) {
      connectionOptionsBuilder.setConnectionPropertyValue(propertyValue);
    }
    return connectionOptionsBuilder;
  }
}
