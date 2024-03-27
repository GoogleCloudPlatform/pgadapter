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

package com.google.cloud.spanner.pgadapter.metadata;

import java.io.IOException;
import java.io.InputStream;

/**
 * Simple {@link InputStream} implementation that forwards all reads to an underlying {@link
 * InputStream}. This class is not thread safe.
 */
public class ForwardingInputStream extends InputStream {
  private InputStream delegate;

  public ForwardingInputStream() {}

  public void setDelegate(InputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public int read() throws IOException {
    if (delegate == null) {
      throw new IOException("No delegate connected");
    }
    return delegate.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (delegate == null) {
      throw new IOException("No delegate connected");
    }
    return delegate.read(b, off, len);
  }
}
