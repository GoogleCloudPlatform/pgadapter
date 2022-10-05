package com.google.cloud.spanner.pgadapter.python.Django;

import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import java.util.Arrays;
import org.junit.BeforeClass;

public class DjangoMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        new MockSpannerServiceImpl(), "d", Arrays.asList("--server-version", "11.1"));
  }
}
