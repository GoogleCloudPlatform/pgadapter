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

package com.google.cloud.spanner.pgadapter.statements;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class JdbcMetadataStatementHelperTest {

  @Test
  public void testApplySpannerQueryTransformations() {
    String input1 = "LIKE 'abc%' ESCAPE '\\'";
    String expected1 = "LIKE 'abc%'";
    assertEquals(expected1, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input1));

    String input2 = "LIKE 'abc%' ESCAPE '*'";
    String expected2 = "LIKE 'abc%'";
    assertEquals(expected2, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input2));

    String input3 = "ILIKE 'a%' ESCAPE ''";
    String expected3 = "ILIKE 'a%'";
    assertEquals(expected3, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input3));

    String input4 =
        "SELECT * FROM table WHERE column LIKE 'xyz%' ESCAPE '\\' AND column2 = 'value'";
    String expected4 = "SELECT * FROM table WHERE column LIKE 'xyz%' AND column2 = 'value'";
    assertEquals(expected4, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input4));

    String input5 = "SELECT * FROM pg_catalog.pg_tables WHERE tablename LIKE 'test%' ESCAPE '\\'";
    String expected5 = "SELECT * FROM pg_catalog.pg_tables WHERE tablename LIKE 'test%'";
    assertEquals(expected5, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input5));

    String input6 =
        "SELECT * FROM table WHERE column LIKE 'test%' ESCAPE 'x' OR column2 LIKE 'product%' ESCAPE '*'";
    String expected6 = "SELECT * FROM table WHERE column LIKE 'test%' OR column2 LIKE 'product%'";
    assertEquals(expected6, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input6));

    String input7 = "DELETE FROM table WHERE column LIKE 'test%' ESCAPE 'a'";
    String expected7 = "DELETE FROM table WHERE column LIKE 'test%'";
    assertEquals(expected7, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input7));

    String input8 = "DELETE FROM table WHERE column LIKE 'ESCAPE%' ESCAPE ''";
    String expected8 = "DELETE FROM table WHERE column LIKE 'ESCAPE%'";
    assertEquals(expected8, JdbcMetadataStatementHelper.applySpannerQueryTransformations(input8));
  }

  @Test
  public void testIsPotentialSpannerQueryModification() {
    assertTrue(
        "Should detect LIKE ESCAPE pattern",
        JdbcMetadataStatementHelper.isPotentialLSpannerQueryModification(
            "SELECT * FROM users WHERE name LIKE 'abc%' ESCAPE '\\'"));

    assertTrue(
        "Should detect multiple LIKE ESCAPE patterns",
        JdbcMetadataStatementHelper.isPotentialLSpannerQueryModification(
            "SELECT * FROM users WHERE name LIKE 'abc%' ESCAPE '\\' AND city LIKE 'NY%' ESCAPE '!'"));

    assertTrue(
        "Should detect LIKE ESCAPE with special character",
        JdbcMetadataStatementHelper.isPotentialLSpannerQueryModification(
            "SELECT * FROM users WHERE name LIKE 'test%' ESCAPE '*'"));

    assertTrue(
        "Should detect LIKE ESCAPE with empty string",
        JdbcMetadataStatementHelper.isPotentialLSpannerQueryModification(
            "SELECT * FROM users WHERE name LIKE 'test%' ESCAPE ''"));

    assertFalse(
        "Should not detect LIKE ESCAPE when there is none",
        JdbcMetadataStatementHelper.isPotentialLSpannerQueryModification(
            "SELECT * FROM users WHERE name LIKE 'abc%'"));

    assertFalse(
        "Should not flag pg_catalog queries as requiring modification",
        JdbcMetadataStatementHelper.isPotentialLSpannerQueryModification(
            "SELECT * FROM pg_catalog.table1"));

    assertFalse(
        "Should not flag queries that don’t contain LIKE ESCAPE",
        JdbcMetadataStatementHelper.isPotentialLSpannerQueryModification(
            "SELECT * FROM employees WHERE department = 'HR'"));
  }
}
