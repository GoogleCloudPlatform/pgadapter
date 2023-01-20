// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.utils;

import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

/** A simple not-thread-safe lazy initializer. */
@NotThreadSafe
public abstract class LazyInit<T> implements Supplier<T> {
  private boolean initialized;
  private T value;

  @Override
  public T get() {
    if (!initialized) {
      initialized = true;
      value = initialize();
    }
    return value;
  }

  /** Initializes the underlying value. Will be called at most once. */
  protected abstract T initialize();
}
