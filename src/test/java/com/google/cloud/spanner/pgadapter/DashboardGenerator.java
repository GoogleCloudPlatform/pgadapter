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

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class DashboardGenerator {

  public static void main(String[] args) throws IOException {
    String templateFile = ".github/test-template.md";
    StringBuilder original = new StringBuilder();
    try (Scanner scanner = new Scanner(new FileReader(templateFile))) {
      while (scanner.hasNextLine()) {
        original.append(scanner.nextLine()).append("\n");
      }
    }
    String output =
        original
            .toString()
            .replace(
                "{{ dashboard.contents }}",
                "This is some random content: " + new Random().nextInt());
    try (FileWriter writer = new FileWriter(templateFile)) {
      writer.write(output);
      writer.flush();
    }
  }
}
