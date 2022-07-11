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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;

/**
 * {@link CopyRecord} is a common interface for COPY data records that are produced by a parser for
 * a specific COPY format.
 */
interface CopyRecord {

  /** Returns the number of columns in the record. */
  int numColumns();

  /**
   * Returns true if the copy record has column names. The {@link #getValue(Type, String)} method
   * can only be used for records that have column names.
   */
  boolean hasColumnNames();

  /**
   * Returns the value of the given column as a Cloud Spanner {@link Value} of the given type. This
   * method is used by a COPY ... FROM ... operation to convert a value to the type of the column
   * where it is being inserted. This method can only be used with records that contains column
   * names.
   */
  Value getValue(Type type, String columnName);

  /**
   * Returns the value of the given column as a Cloud Spanner {@link Value} of the given type. This
   * method is used by a COPY ... FROM ... operation to convert a value to the type of the column
   * where it is being inserted. This method is supported for all types of {@link CopyRecord}.
   */
  Value getValue(Type type, int columnIndex);
}
