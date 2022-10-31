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

package com.google.cloud.spanner.pgadapter.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CopySettingsTest {

  @Test
  public void testMaxParallelism() {
    try {
      System.setProperty("copy_in_max_parallelism", "15");
      SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
      CopySettings copySettings = new CopySettings(sessionState);
      assertEquals(15, copySettings.getMaxParallelism());
    } finally {
      System.clearProperty("copy_in_max_parallelism");
    }
    SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
    CopySettings copySettings = new CopySettings(sessionState);
    assertEquals(128, copySettings.getMaxParallelism());

    sessionState.set("spanner", "copy_max_parallelism", "256");
    assertEquals(copySettings.getMaxParallelism(), 256);
  }

  @Test
  public void testUpsert() {
    try {
      System.setProperty("copy_in_insert_or_update", "true");
      SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
      CopySettings copySettings = new CopySettings(sessionState);
      assertTrue(copySettings.isCopyUpsert());
    } finally {
      System.clearProperty("copy_in_insert_or_update");
    }
    SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
    CopySettings copySettings = new CopySettings(sessionState);
    assertFalse(copySettings.isCopyUpsert());

    sessionState.set("spanner", "copy_upsert", "true");
    assertTrue(copySettings.isCopyUpsert());
  }

  @Test
  public void testMutationLimit() {
    try {
      System.setProperty("copy_in_mutation_limit", "100");
      SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
      CopySettings copySettings = new CopySettings(sessionState);
      assertEquals(100, copySettings.getMaxAtomicMutationsLimit());
      assertEquals(100, copySettings.getNonAtomicBatchSize());
    } finally {
      System.clearProperty("copy_in_mutation_limit");
    }
    SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
    CopySettings copySettings = new CopySettings(sessionState);
    assertEquals(40_000, copySettings.getMaxAtomicMutationsLimit());
    assertEquals(5000, copySettings.getNonAtomicBatchSize());

    assertThrows(
        SpannerException.class,
        () -> sessionState.set("spanner", "copy_max_atomic_mutations", "1000"));
    sessionState.set("spanner", "copy_batch_size", "1000");
    assertEquals(1000, copySettings.getNonAtomicBatchSize());
  }

  @Test
  public void testCommitSizeLimit() {
    try {
      System.setProperty("copy_in_commit_limit", "1000");
      SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
      CopySettings copySettings = new CopySettings(sessionState);
      assertEquals(1000, copySettings.getMaxAtomicCommitSize());
      assertEquals(1000, copySettings.getMaxNonAtomicCommitSize());
    } finally {
      System.clearProperty("copy_in_commit_limit");
    }
    SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
    CopySettings copySettings = new CopySettings(sessionState);
    assertEquals(100_000_000, copySettings.getMaxAtomicCommitSize());
    assertEquals(5_000_000, copySettings.getMaxNonAtomicCommitSize());

    assertThrows(
        SpannerException.class,
        () -> sessionState.set("spanner", "copy_max_atomic_commit_size", "1000"));
    sessionState.set("spanner", "copy_max_non_atomic_commit_size", "1000");
    assertEquals(1000, copySettings.getMaxNonAtomicCommitSize());
  }

  @Test
  public void testCommitLimitMultiplierFactor() {
    try {
      System.setProperty("copy_in_commit_limit_multiplier_factor", "1.5");
      SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
      CopySettings copySettings = new CopySettings(sessionState);
      assertEquals(1.5f, copySettings.getCommitSizeMultiplier(), 0.0f);
    } finally {
      System.clearProperty("copy_in_commit_limit_multiplier_factor");
    }
    SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
    CopySettings copySettings = new CopySettings(sessionState);
    assertEquals(2.0f, copySettings.getCommitSizeMultiplier(), 0.0f);

    sessionState.set("spanner", "copy_commit_size_multiplier_factor", "3.0");
    assertEquals(3.0f, copySettings.getCommitSizeMultiplier(), 0.0f);
  }

  @Test
  public void testPipeBufferSize() {
    try {
      System.setProperty("copy_in_pipe_buffer_size", "5000");
      SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
      CopySettings copySettings = new CopySettings(sessionState);
      assertEquals(5000, copySettings.getPipeBufferSize());
    } finally {
      System.clearProperty("copy_in_pipe_buffer_size");
    }
    SessionState sessionState = new SessionState(mock(OptionsMetadata.class));
    CopySettings copySettings = new CopySettings(sessionState);
    assertEquals(1 << 16, copySettings.getPipeBufferSize());

    assertThrows(
        SpannerException.class, () -> sessionState.set("spanner", "copy_pipe_buffer_size", "1000"));
  }
}
