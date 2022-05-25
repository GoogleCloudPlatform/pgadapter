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

package com.google.cloud.spanner.pgadapter.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OptionsMetadataTest {

  @Test
  public void testDefaultDomainSocketFile() {
    for (String os : new String[] {"ubuntu", "windows"}) {
      OptionsMetadata options = new OptionsMetadata(os, new String[] {"-p p", "-i i"});
      if (options.isWindows()) {
        assertEquals("", options.getSocketFile(5432));
        assertFalse(options.isDomainSocketEnabled());
      } else {
        assertEquals("/var/run/postgresql/.s.PGSQL.5432", options.getSocketFile(5432));
        assertTrue(options.isDomainSocketEnabled());
      }
    }
  }

  @Test
  public void testCustomDomainSocketFile() {
    for (String os : new String[] {"ubuntu", "windows"}) {
      OptionsMetadata options =
          new OptionsMetadata(os, new String[] {"-p p", "-i i", "-f /tmp/.my-socket.%d"});
      assertEquals("/tmp/.my-socket.5432", options.getSocketFile(5432));
      assertTrue(options.isDomainSocketEnabled());
    }
  }
}
