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
import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

interface ServerSocketFactory {
  ServerSocketChannel createTcpServerSocketChannel(OptionsMetadata options) throws IOException;

  ServerSocketChannel creatUnixDomainSocketChannel(OptionsMetadata options, int localPort)
      throws IOException;
}
