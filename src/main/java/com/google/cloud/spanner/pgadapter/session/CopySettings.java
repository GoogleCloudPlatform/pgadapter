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

import com.google.cloud.spanner.Options.RpcPriority;
import java.util.Locale;
import java.util.Map;

public class CopySettings {
  private static final int DEFAULT_PIPE_BUFFER_SIZE = 1 << 16;
  private static final int DEFAULT_MAX_MUTATION_LIMIT = 20_000; // 20k mutations limit

  private final SessionState sessionState;

  static void initCopySettings(Map<String, PGSetting> settings) {
    // COPY settings could originally be set using System properties. Use these system properties as
    // the default if they have been set.
    if (System.getProperty("copy_in_max_parallelism") != null) {
      int maxParallelism = Integer.parseInt(System.getProperty("copy_in_max_parallelism"));
      settings
          .get("spanner.copy_max_parallelism")
          .initSettingValue(String.valueOf(Math.max(maxParallelism, 1)));
    }
    if (System.getProperty("copy_in_insert_or_update") != null) {
      settings
          .get("spanner.copy_upsert")
          .initSettingValue(System.getProperty("copy_in_insert_or_update"));
    }
    if (System.getProperty("copy_in_mutation_limit") != null) {
      int mutationLimit = Integer.parseInt(System.getProperty("copy_in_mutation_limit"));
      settings
          .get("spanner.copy_max_atomic_mutations")
          .initSettingValue(String.valueOf(Math.max(mutationLimit, 1)));
      settings
          .get("spanner.copy_batch_size")
          .initSettingValue(String.valueOf(Math.max(mutationLimit, 1)));
    }
    if (System.getProperty("copy_in_commit_limit") != null) {
      int commitSizeLimit = Integer.parseInt(System.getProperty("copy_in_commit_limit"));
      settings
          .get("spanner.copy_max_atomic_commit_size")
          .initSettingValue(String.valueOf(commitSizeLimit));
      settings
          .get("spanner.copy_max_non_atomic_commit_size")
          .initSettingValue(String.valueOf(commitSizeLimit));
    }
    if (System.getProperty("copy_in_commit_limit_multiplier_factor") != null) {
      float commitLimitMultiplierFactor =
          Float.parseFloat(System.getProperty("copy_in_commit_limit_multiplier_factor"));
      settings
          .get("spanner.copy_commit_size_multiplier_factor")
          .initSettingValue(String.valueOf(Math.max(commitLimitMultiplierFactor, 1.0f)));
    }
    if (System.getProperty("copy_in_pipe_buffer_size") != null) {
      int pipeBufferSize =
          Integer.parseInt(
              System.getProperty(
                  "copy_in_pipe_buffer_size", String.valueOf(DEFAULT_PIPE_BUFFER_SIZE)));
      settings
          .get("spanner.copy_pipe_buffer_size")
          .initSettingValue(String.valueOf(Math.max(pipeBufferSize, 1024)));
    }
  }

  public CopySettings(SessionState sessionState) {
    this.sessionState = sessionState;
  }

  /** Returns the maximum number of parallel transactions for a single COPY operation. */
  public int getMaxParallelism() {
    return sessionState.getIntegerSetting("spanner", "copy_max_parallelism", 128);
  }

  /** Returns the commit timeout for COPY operations in seconds. */
  public int getCommitTimeoutSeconds() {
    return sessionState.getIntegerSetting("spanner", "copy_commit_timeout", 300);
  }

  /** Returns the request priority for commits executed by COPY operations. */
  public RpcPriority getCommitPriority() {
    String setting =
        sessionState
            .getStringSetting("spanner", "copy_commit_priority", "medium")
            .toUpperCase(Locale.ENGLISH);
    try {
      return RpcPriority.valueOf(setting);
    } catch (IllegalArgumentException e) {
      return RpcPriority.MEDIUM;
    }
  }

  /** Returns the batch size to use for non-atomic COPY operations. */
  public int getNonAtomicBatchSize() {
    return sessionState.getIntegerSetting("spanner", "copy_batch_size", 5000);
  }

  /** Returns the maximum number of mutations in a single commit request. */
  public int getMaxAtomicMutationsLimit() {
    return sessionState.getIntegerSetting("spanner", "copy_max_atomic_mutations", 20_000);
  }

  /** Returns the maximum number of bytes in a single commit request. */
  public int getMaxAtomicCommitSize() {
    return sessionState.getIntegerSetting("spanner", "copy_max_atomic_commit_size", 100_000_000);
  }

  /**
   * Returns the maximum number of bytes in a single commit request for non-atomic COPY operations.
   */
  public int getMaxNonAtomicCommitSize() {
    return sessionState.getIntegerSetting("spanner", "copy_max_non_atomic_commit_size", 5_000_000);
  }

  /**
   * Returns the multiplier that will be applied to the calculated commit size to ensure that a
   * commit request in a non-atomic COPY operation does not exceed the fixed commit size limit of
   * Cloud Spanner.
   */
  public float getCommitSizeMultiplier() {
    return sessionState.getFloatSetting("spanner", "copy_commit_size_multiplier_factor", 2.0f);
  }

  /** Returns the buffer size to use for incoming COPY data messages. */
  public int getPipeBufferSize() {
    return sessionState.getIntegerSetting("spanner", "copy_pipe_buffer_size", 1 << 16);
  }

  /**
   * Returns whether COPY operations should use upsert instead of insert.
   *
   * <p>COPY will INSERT records by default. This is consistent with how COPY on PostgreSQL works.
   * This option allows PGAdapter to use InsertOrUpdate instead. This can be slightly more efficient
   * for bulk uploading, and it makes it easier to retry a failed non-atomic batch that might have
   * already uploaded some but not all data.
   */
  public boolean isCopyUpsert() {
    return sessionState.getBoolSetting("spanner", "copy_upsert", false);
  }
}
