// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.DamlOptional;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Int64;
import static com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy.*;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class PrimitiveValueDecodersTest {

  private static final DamlRecord RECORD_WITH_2_FIELDS =
      new DamlRecord(new DamlRecord.Field(new Int64(1L)), new DamlRecord.Field(new Int64(2L)));

  private static final DamlRecord RECORD_WITH_3_FIELDS =
      new DamlRecord(
          new DamlRecord.Field(new Int64(1L)),
          new DamlRecord.Field(new Int64(2L)),
          new DamlRecord.Field(new Int64(3L)));

  private static final List<OptionalUpgradeTestCase> OPTIONAL_UPGRADE_TEST_CASES =
      List.of(
          new OptionalUpgradeTestCase(4, 2, RECORD_WITH_2_FIELDS, STRICT, 4),
          new OptionalUpgradeTestCase(4, 2, RECORD_WITH_3_FIELDS, STRICT, 4),
          new OptionalUpgradeTestCase(4, 2, RECORD_WITH_2_FIELDS, IGNORE, 4),
          new OptionalUpgradeTestCase(4, 2, RECORD_WITH_3_FIELDS, IGNORE, 4));

  @Test
  public void testRecordCheckExactNumberOfFields() {
    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord.Field f2 = new DamlRecord.Field(new Int64(2L));
    DamlRecord record = new DamlRecord(f1, f2);

    PreparedRecord pr = PrimitiveValueDecoders.checkAndPrepareRecord(2, 0, record, STRICT);
    List<DamlRecord.Field> expected = pr.getExpectedFields();
    assertEquals(2, expected.size());
  }

  @Test
  public void testRecordCheckWithMoreTrailingOptionalFieldsThanExpected() {
    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord.Field f2 = new DamlRecord.Field(DamlOptional.of(new Int64(2L)));
    DamlRecord record = new DamlRecord(f1, f2);

    PreparedRecord pr = PrimitiveValueDecoders.checkAndPrepareRecord(1, 0, record, IGNORE);
    List<DamlRecord.Field> expected = pr.getExpectedFields();
    assertEquals(1, expected.size());
  }

  @Test
  public void testRecordCheckWithTrailingEmptyOptionals() {
    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord.Field f2 = new DamlRecord.Field(DamlOptional.EMPTY);
    DamlRecord record = new DamlRecord(f1, f2);

    PreparedRecord pr = PrimitiveValueDecoders.checkAndPrepareRecord(1, 1, record, STRICT);
    List<DamlRecord.Field> expected = pr.getExpectedFields();
    assertEquals(1, expected.size());
  }

  @Test
  public void testRecordCheckFailsWhenExtraNotOptional() {
    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord.Field f2 = new DamlRecord.Field(new Int64(2L));
    DamlRecord record = new DamlRecord(f1, f2);

    assertThrows(
        IllegalArgumentException.class,
        () -> PrimitiveValueDecoders.checkAndPrepareRecord(1, 1, record, STRICT));
  }

  @Test
  public void testRecordCheckUpgradeAppendsEmptyOptionals() {
    OPTIONAL_UPGRADE_TEST_CASES.forEach(
        testCase -> {
          PreparedRecord pr =
              PrimitiveValueDecoders.checkAndPrepareRecord(
                  testCase.expectedFields,
                  testCase.expectedTrailingOptionals,
                  testCase.record,
                  testCase.policy);
          List<DamlRecord.Field> expected = pr.getExpectedFields();
          assertEquals(testCase.resultFieldsCount, expected.size());
          int recordFieldsCount = testCase.record.getFields().size();
          for (int i = 0; i < recordFieldsCount; i++) {
            assertEquals(expected.get(i).getValue(), testCase.record.getFields().get(i).getValue());
          }
          for (int j = recordFieldsCount; j < testCase.resultFieldsCount; j++) {
            assertTrue(expected.get(j).getValue().asOptional().get().isEmpty());
          }
        });

    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord record = new DamlRecord(f1);

    PreparedRecord pr = PrimitiveValueDecoders.checkAndPrepareRecord(3, 2, record, STRICT);
    List<DamlRecord.Field> expected = pr.getExpectedFields();
    assertEquals(3, expected.size());
    assertSame(f1, expected.get(0));
    assertTrue(expected.get(1).getValue() instanceof DamlOptional);
    assertTrue(expected.get(2).getValue() instanceof DamlOptional);
    assertTrue(((DamlOptional) expected.get(1).getValue()).isEmpty());
    assertTrue(((DamlOptional) expected.get(2).getValue()).isEmpty());
  }

  @Test
  public void testRecordCheckUpgradeFailsWhenMissingNonOptionalFields() {
    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord record = new DamlRecord(f1);

    assertThrows(
        IllegalArgumentException.class,
        () -> PrimitiveValueDecoders.checkAndPrepareRecord(3, 1, record, STRICT));
  }

  @Test
  public void testStrictPolicyFailsWhenUnknownTrailingNonEmptyOptionalsPresent() {
    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord.Field f2 = new DamlRecord.Field(DamlOptional.of(new Int64(2L)));
    DamlRecord.Field f3 = new DamlRecord.Field(DamlOptional.EMPTY);
    DamlRecord record = new DamlRecord(f1, f2, f3);

    assertThrows(
        IllegalArgumentException.class,
        () -> PrimitiveValueDecoders.checkAndPrepareRecord(1, 1, record, STRICT));
  }

  @Test
  public void testIgnorePolicyAllowsUnknownTrailingNonEmptyOptionals() {
    DamlRecord.Field f1 = new DamlRecord.Field(new Int64(1L));
    DamlRecord.Field f2 = new DamlRecord.Field(DamlOptional.of(new Int64(2L)));
    DamlRecord.Field f3 = new DamlRecord.Field(DamlOptional.EMPTY);
    DamlRecord record = new DamlRecord(f1, f2, f3);

    PreparedRecord pr = PrimitiveValueDecoders.checkAndPrepareRecord(1, 1, record, IGNORE);
    List<DamlRecord.Field> expected = pr.getExpectedFields();
    assertEquals(1, expected.size());
    assertSame(f1, expected.get(0));
  }

  private static class OptionalUpgradeTestCase {
    public final int expectedFields;
    public final int expectedTrailingOptionals;
    public final DamlRecord record;
    public final UnknownTrailingFieldPolicy policy;
    public final int resultFieldsCount;

    private OptionalUpgradeTestCase(
        int expectedFields,
        int expectedTrailingOptionals,
        DamlRecord record,
        UnknownTrailingFieldPolicy policy,
        int resultFieldsCount) {
      this.expectedFields = expectedFields;
      this.expectedTrailingOptionals = expectedTrailingOptionals;
      this.record = record;
      this.policy = policy;
      this.resultFieldsCount = resultFieldsCount;
    }
  }
}
