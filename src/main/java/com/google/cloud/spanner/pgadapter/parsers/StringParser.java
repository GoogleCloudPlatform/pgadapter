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

package com.google.cloud.spanner.pgadapter.parsers;

import java.sql.ResultSet;
import java.sql.SQLException;

/** Translate from wire protocol to string. */
public class StringParser extends Parser<String> {

  public StringParser(ResultSet item, int position) throws SQLException {
    this.item = item.getString(position);
  }

  public StringParser(Object item) {
    this.item = (String) item;
  }

  public StringParser(byte[] item, FormatCode formatCode) {
    this.item = new String(item, UTF8);
  }

  @Override
  public String getItem() {
    return this.item;
  }

  @Override
  protected String stringParse() {
    return this.item;
  }
}
