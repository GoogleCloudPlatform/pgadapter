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

package com.google.cloud.spanner.pgadapter.channels;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.spanner.connection.ConnectionOptions.ExternalChannelProvider;
import com.google.common.base.Strings;

public class TestChannelWithCertificatesProvider implements ExternalChannelProvider {
  private static final String TEST_CERT_PROPERTY = "TEST_CERT";
  private static final String HOST_IN_CERT_PROPERTY = "TEST_HOST_IN_CERT";

  @Override
  public TransportChannelProvider getChannelProvider(String host, int port) {
    if ((!Strings.isNullOrEmpty(System.getProperty(TEST_CERT_PROPERTY))
            && Strings.isNullOrEmpty(System.getProperty(HOST_IN_CERT_PROPERTY)))
        || (Strings.isNullOrEmpty(System.getProperty(TEST_CERT_PROPERTY))
            && !Strings.isNullOrEmpty(System.getProperty(HOST_IN_CERT_PROPERTY)))) {
      throw new IllegalArgumentException(
          String.format(
              "%s and %s must both be set to non-null non-empty values, or both not be set",
              TEST_CERT_PROPERTY, HOST_IN_CERT_PROPERTY));
    }
    if (Strings.isNullOrEmpty(System.getProperty(TEST_CERT_PROPERTY))) {
      return FixedTransportChannelProvider.create(
          GrpcTransportChannel.create(
              TestChannelWithCertificates.getChannelForTestHost(host, port)));
    }
    return FixedTransportChannelProvider.create(
        GrpcTransportChannel.create(
            TestChannelWithCertificates.getChannelForTestHost(
                host,
                port,
                System.getProperty(TEST_CERT_PROPERTY),
                System.getProperty(HOST_IN_CERT_PROPERTY))));
  }
}
