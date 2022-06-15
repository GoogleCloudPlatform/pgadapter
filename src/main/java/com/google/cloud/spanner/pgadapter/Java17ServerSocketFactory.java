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
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;

// If your IDE is complaining about this file not being compatible with the language level
// of the project, then that is an indication that the IDE did not pick up the exclusion of this
// file that is defined in the pom.xml.
// This is a known issue in IntelliJ: https://youtrack.jetbrains.com/issue/IDEA-87868
//
// Follow these steps to make IntelliJ ignore this compilation error:
// 1. Go to Settings.
// 2. Select Build, Execution, Deployment| Compiler | Excludes
// 3. Add this file to the list of excludes.
// See also https://www.jetbrains.com/help/idea/specifying-compilation-settings.html#5a737cfc

class Java17ServerSocketFactory implements ServerSocketFactory {

  public Java17ServerSocketFactory() {}

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
    File socketFile = new File(options.getSocketFile(localPort));
    Path path = Path.of(socketFile.toURI());
    if (socketFile.getParentFile() != null && !socketFile.getParentFile().exists()) {
      socketFile.mkdirs();
    }

    // deleteOnExit() does not work when PGAdapter is built as a native image. It also does not get
    // deleted if the JVM crashes. We therefore need to try to delete the file at startup to ensure
    // we don't get a lot of 'address already in use' errors.
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignore) {
      // Ignore and let the Unix domain socket subsystem throw an 'Address in use' error.
    }

    UnixDomainSocketAddress address = UnixDomainSocketAddress.of(path);
    ServerSocketChannel domainSocketChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
    domainSocketChannel.configureBlocking(true);
    domainSocketChannel.bind(address, options.getMaxBacklog());

    return domainSocketChannel;
  }
}
