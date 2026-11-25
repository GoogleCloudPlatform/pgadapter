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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.connection.ConnectionOptions.StartupPropertyPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConnectionOptionsHelperTest {

  public static Connection setupMockConnection(Spanner spanner) {
    ConnectionImpl connection = mock(ConnectionImpl.class);
    when(connection.getSpanner()).thenReturn(spanner);

    return connection;
  }

  public static <T> T getInitialConnectionPropertyValue(
      ConnectionOptions options, ConnectionProperty<T> property) {
    return options.getInitialConnectionPropertyValue(property);
  }

  @Test
  public void testSetCredentials() {
    ConnectionOptions.Builder builder = mock(ConnectionOptions.Builder.class);
    GoogleCredentials credentials = new GoogleCredentials() {};

    PGAdapterConnectionOptionsHelper.setCredentials(builder, credentials);

    verify(builder).setCredentials(credentials);
  }

  @Test
  public void testAddPropertiesFromServerUri() {
    String baseUri = "cloudspanner:/projects/p/instances/i/databases/d";
    String serverUri = baseUri + ";userAgent=pg-adapter;numChannels=4;dialect=postgresql";
    ConnectionOptions.Builder builder =
        ConnectionOptions.newBuilder()
            .setStartupPropertyPolicy(StartupPropertyPolicy.REJECT)
            .setCredentials(NoCredentials.getInstance());
    PGAdapterConnectionOptionsHelper.addPropertiesFromServerUri(builder, serverUri);
    builder.setUri(baseUri);
    ConnectionOptions connectionOptions = builder.build();

    assertEquals(Integer.valueOf(4), connectionOptions.getNumChannels());
    assertEquals(Dialect.POSTGRESQL, connectionOptions.getDialect());
    assertEquals("pg-adapter", connectionOptions.getUserAgent());

    // '?' delimiter is also supported.
    ConnectionOptions.Builder questionMarkBuilder =
        ConnectionOptions.newBuilder()
            .setStartupPropertyPolicy(StartupPropertyPolicy.REJECT)
            .setCredentials(NoCredentials.getInstance());
    PGAdapterConnectionOptionsHelper.addPropertiesFromServerUri(
        questionMarkBuilder, baseUri + "?numChannels=3;dialect=postgresql");
    ConnectionOptions questionMarkOptions = questionMarkBuilder.setUri(baseUri).build();
    assertEquals(Integer.valueOf(3), questionMarkOptions.getNumChannels());
    assertEquals(Dialect.POSTGRESQL, questionMarkOptions.getDialect());

    // URI with no properties is a no-op.
    ConnectionOptions.Builder noPropsBuilder =
        ConnectionOptions.newBuilder()
            .setStartupPropertyPolicy(StartupPropertyPolicy.REJECT)
            .setCredentials(NoCredentials.getInstance());
    PGAdapterConnectionOptionsHelper.addPropertiesFromServerUri(noPropsBuilder, baseUri);
    ConnectionOptions noPropsOptions = noPropsBuilder.setUri(baseUri).build();
    assertNull(noPropsOptions.getNumChannels());

    // Unknown property fails when lenient=false, and is accepted when lenient=true.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PGAdapterConnectionOptionsHelper.addPropertiesFromServerUri(
                ConnectionOptions.newBuilder(), baseUri + ";unknownProp=foo"));
    ConnectionOptions.Builder lenientBuilder =
        ConnectionOptions.newBuilder()
            .setStartupPropertyPolicy(StartupPropertyPolicy.REJECT)
            .setCredentials(NoCredentials.getInstance());
    PGAdapterConnectionOptionsHelper.addPropertiesFromServerUri(
        lenientBuilder, baseUri + ";lenient=true;unknownProp=foo;numChannels=2");
    ConnectionOptions lenientOptions = lenientBuilder.setUri(baseUri).build();
    assertEquals(Integer.valueOf(2), lenientOptions.getNumChannels());

    // Invalid property value fails with INVALID_ARGUMENT.
    SpannerException invalidValueException =
        assertThrows(
            SpannerException.class,
            () ->
                PGAdapterConnectionOptionsHelper.addPropertiesFromServerUri(
                    ConnectionOptions.newBuilder(), baseUri + ";minSessions=-1"));
    assertEquals(ErrorCode.INVALID_ARGUMENT, invalidValueException.getErrorCode());

    // Invalid lenient property value also fails with INVALID_ARGUMENT.
    SpannerException invalidLenientException =
        assertThrows(
            SpannerException.class,
            () ->
                PGAdapterConnectionOptionsHelper.addPropertiesFromServerUri(
                    ConnectionOptions.newBuilder(), baseUri + ";lenient=invalid"));
    assertEquals(ErrorCode.INVALID_ARGUMENT, invalidLenientException.getErrorCode());
  }

  @Test
  public void testIsValidPropertyValue() {
    assertTrue(
        PGAdapterConnectionOptionsHelper.isValidPropertyValue(
            ConnectionProperties.NUM_CHANNELS, "2"));
    assertFalse(
        PGAdapterConnectionOptionsHelper.isValidPropertyValue(
            ConnectionProperties.NUM_CHANNELS, "-1"));
    assertFalse(
        PGAdapterConnectionOptionsHelper.isValidPropertyValue(
            ConnectionProperties.NUM_CHANNELS, null));
    assertTrue(
        PGAdapterConnectionOptionsHelper.isValidPropertyValue(
            ConnectionProperties.DATABASE_ROLE, "my_role"));
    assertFalse(
        PGAdapterConnectionOptionsHelper.isValidPropertyValue(
            ConnectionProperties.DATABASE_ROLE, null));
  }
}
