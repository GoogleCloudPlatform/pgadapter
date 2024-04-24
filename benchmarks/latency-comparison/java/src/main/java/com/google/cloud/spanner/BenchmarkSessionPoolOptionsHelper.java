package com.google.cloud.spanner;

public class BenchmarkSessionPoolOptionsHelper {

  public static SessionPoolOptions getSessionPoolOptions(boolean useMultiplexedSessions) {
    return SessionPoolOptions.newBuilder().setUseMultiplexedSession(useMultiplexedSessions).build();
  }

}
