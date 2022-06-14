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

package com.google.cloud.spanner.pgadapter;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import org.newsclub.net.unix.AFUNIXServerSocketChannel;
import org.newsclub.net.unix.AFUNIXSocketAddress;

class Java8ServerSocketFactory implements ServerSocketFactory {

  public Java8ServerSocketFactory() {}

  public ServerSocketChannel createTcpServerSocketChannel(OptionsMetadata options)
      throws IOException {
    ServerSocketChannel channel = ServerSocketChannel.open();
    InetSocketAddress address = new InetSocketAddress(options.getProxyPort());
    channel.configureBlocking(true);
    channel.bind(address, options.getMaxBacklog());

    return channel;
  }

  public ServerSocketChannel creatUnixDomainSocketChannel(OptionsMetadata options, int localPort)
      throws IOException {
    File tempDir = new File(options.getSocketFile(localPort));
    if (tempDir.getParentFile() != null && !tempDir.getParentFile().exists()) {
      tempDir.mkdirs();
    }
    AFUNIXServerSocketChannel channel = AFUNIXServerSocketChannel.open();
    channel.bind(AFUNIXSocketAddress.of(tempDir), options.getMaxBacklog());

    return channel;
  }
}
