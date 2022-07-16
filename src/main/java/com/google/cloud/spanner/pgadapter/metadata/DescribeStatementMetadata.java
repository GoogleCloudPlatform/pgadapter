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
import com.google.cloud.Tuple;
import com.google.cloud.spanner.ResultSet;

/** Simple POJO to hold describe metadata specific to prepared statements. */
@InternalApi
public class DescribeStatementMetadata extends DescribeMetadata<Tuple<int[], ResultSet>>
    implements AutoCloseable {

  public DescribeStatementMetadata(int[] parameters, ResultSet resultMetaData) {
    this.metadata = Tuple.of(parameters, resultMetaData);
  }

  public int[] getParameters() {
    return metadata.x();
  }

  public ResultSet getResultSet() {
    return metadata.y();
  }

  @Override
  public void close() {
    if (getResultSet() != null) {
      getResultSet().close();
    }
  }
}
