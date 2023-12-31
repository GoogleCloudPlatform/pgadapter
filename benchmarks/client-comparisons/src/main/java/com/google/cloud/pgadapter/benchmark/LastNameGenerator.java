package com.google.cloud.pgadapter.benchmark;

import com.google.common.collect.ImmutableList;
import java.util.Random;

public class LastNameGenerator {
  private static final ImmutableList<String> LAST_NAME_PARTS =
      ImmutableList.of(
          "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING");

  public static String generateLastName(Random random, long rowIndex) {
    int row;
    if (rowIndex < 1000L) {
      row = (int) rowIndex;
    } else {
      row = random.nextInt(1000);
    }
    return LAST_NAME_PARTS.get(row / 100)
        + LAST_NAME_PARTS.get((row / 10) % 10)
        + LAST_NAME_PARTS.get(row % 10);
  }
}
