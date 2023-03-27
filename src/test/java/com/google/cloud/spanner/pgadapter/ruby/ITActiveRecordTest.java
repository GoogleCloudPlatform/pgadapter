package com.google.cloud.spanner.pgadapter.ruby;

import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITActiveRecordTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
}
