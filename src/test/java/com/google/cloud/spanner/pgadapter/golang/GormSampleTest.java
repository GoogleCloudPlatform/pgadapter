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

package com.google.cloud.spanner.pgadapter.golang;

import com.sun.jna.Library;

/**
 * Interface for the gorm sample integration tests. Each method in this class represents a test
 * method in samples/golang/gorm/sample.go.
 */
public interface GormSampleTest extends Library {

  String TestRunSample(GoString connString, GoString directory);
}
