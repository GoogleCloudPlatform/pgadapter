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

package com.google.cloud.spanner.myadapter.channels;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.net.HostAndPort;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.InternalNettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;

/** Utility functions for creating GRPC Channels that require certificates. */
public final class TestChannelWithCertificates {

  private TestChannelWithCertificates() {}

  private static final String DEFAULT_TEST_CERT = "net/httpsconnection/testdata/CAcert.pem";
  private static final String DEFAULT_HOST_IN_CERT = "test_cert_2";

  /**
   * Create a channel to a service running behind a default test host configuration.
   *
   * @param host name running the test host, typically localhost.
   * @param sslPort of the test host which must have HTTP2 enabled.
   * @return a connected {@link Channel}
   * @throws java.lang.RuntimeException if unable to create the channel
   */
  public static ManagedChannel getChannelForTestHost(String host, int sslPort) {
    return getChannelForTestHost(host, sslPort, DEFAULT_TEST_CERT, DEFAULT_HOST_IN_CERT);
  }

  /**
   * Create a channel builder to a service running behind a default test host configuration.
   *
   * @param host name running the test host, typically localhost.
   * @param sslPort of the test host which must have HTTP2 enabled.
   * @return a connected {@link Channel}
   * @throws java.lang.RuntimeException if unable to create the channel
   */
  public static ManagedChannelBuilder<?> getChannelBuilderForTestHost(String host, int sslPort) {
    return getChannelBuilderForTestHost(host, sslPort, DEFAULT_TEST_CERT, DEFAULT_HOST_IN_CERT);
  }

  /**
   * Create a channel to a service running behind the specified test host configuration.
   *
   * @param host name running the test host, typically localhost.
   * @param sslPort of the test host which must have HTTP2 enabled.
   * @param certPath path to the certificate file
   * @param hostInCert the name of the test host within the certificate file
   * @return a connected {@link Channel}
   * @throws java.lang.RuntimeException if unable to create the channel
   */
  public static ManagedChannel getChannelForTestHost(
      String host, int sslPort, String certPath, String hostInCert) {
    return getChannelBuilderForTestHost(host, sslPort, certPath, hostInCert).build();
  }

  /**
   * Create a channel builder to a service running behind the specified test host.
   *
   * @param host name running the test host, typically localhost.
   * @param sslPort of the test host which must have HTTP2 enabled.
   * @param certPath path to the certificate file
   * @param hostInCert the name of the test host within the certificate file
   * @return a connected {@link Channel}
   * @throws java.lang.RuntimeException if unable to create the channel
   */
  public static ManagedChannelBuilder<?> getChannelBuilderForTestHost(
      String host, int sslPort, String certPath, String hostInCert) {
    SslContext sslContext;
    try {
      sslContext =
          GrpcSslContexts.forClient()
              .trustManager(CertUtil.copyCert(certPath))
              .ciphers(null)
              .build();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    HostAndPort hostPort = HostAndPort.fromParts(host, sslPort);
    String target;
    try {
      target = new URI("dns", "", "/" + hostPort, null).toString();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
    try {
      NettyChannelBuilder channelBuilder = NettyChannelBuilder.forTarget(target);
      InternalNettyChannelBuilder.disableCheckAuthority(channelBuilder);

      return channelBuilder
          .overrideAuthority(hostInCert)
          .sslContext(sslContext)
          .negotiationType(NegotiationType.TLS);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  static final class CertUtil {
    private CertUtil() {
      // prevent instantiation
    }

    /** Copies cert resource to file, stripping out PEM comments. */
    public static File copyCert(String certFileName) throws IOException {
      File certFile = new File(certFileName);
      File file = File.createTempFile("CAcert", "pem");
      file.deleteOnExit();
      try (BufferedReader in =
              new BufferedReader(new InputStreamReader(new FileInputStream(certFile), UTF_8));
          Writer out = new OutputStreamWriter(new FileOutputStream(file), UTF_8)) {
        String line;
        do {
          while ((line = in.readLine()) != null) {
            if ("-----BEGIN CERTIFICATE-----".equals(line)) {
              break;
            }
          }
          out.append(line);
          out.append("\n");
          while ((line = in.readLine()) != null) {
            out.append(line);
            out.append("\n");
            if ("-----END CERTIFICATE-----".equals(line)) {
              break;
            }
          }
        } while (line != null);
      }
      return file;
    }
  }
}
