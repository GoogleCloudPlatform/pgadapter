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

package com.google.cloud.spanner.pgadapter.statements;

import java.util.List;

public class MoveStatement extends AbstractFetchOrMoveStatement {

  public MoveStatement(
      String name,
      IntermediatePreparedStatement preparedStatement,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    super(name, preparedStatement, parameters, parameterFormatCodes, resultFormatCodes, null);
  }

  @Override
  public String getCommandTag() {
    return "MOVE";
  }

  static class ParsedMoveStatement extends ParsedFetchOrMoveStatement {
    ParsedMoveStatement(String name, Direction direction, Long count) {
      super(name, direction, count);
    }
  }
}
