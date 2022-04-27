// Copyright 2020 Google LLC
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

import com.google.api.core.InternalApi;

/** The state of a result after (a part of it) has been sent to the client. */
@InternalApi
public class SendResultSetState {

  private final long numRowsSent;
  private final boolean moreRows;

  public SendResultSetState(long numRowsSent, boolean hasMoreRows) {
    this.numRowsSent = numRowsSent;
    this.moreRows = hasMoreRows;
  }

  public boolean hasMoreRows() {
    return this.moreRows;
  }

  public long getNumberOfRowsSent() {
    return this.numRowsSent;
  }
}
