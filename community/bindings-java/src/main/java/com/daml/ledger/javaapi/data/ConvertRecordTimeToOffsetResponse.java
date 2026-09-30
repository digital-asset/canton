// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;

import java.util.Objects;

public class ConvertRecordTimeToOffsetResponse {
  private final long offset;

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    ConvertRecordTimeToOffsetResponse that = (ConvertRecordTimeToOffsetResponse) o;
    return offset == that.offset;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(offset);
  }

  @Override
  public String toString() {
    return "ConvertRecordTimeToOffsetResponse{" + "offset=" + offset + '}';
  }

  public ConvertRecordTimeToOffsetResponse(long offset) {
    this.offset = offset;
  }

  public static ConvertRecordTimeToOffsetResponse fromProto(
      StateServiceOuterClass.ConvertRecordTimeToOffsetResponse response) {
    return new ConvertRecordTimeToOffsetResponse(response.getOffset());
  }

  public StateServiceOuterClass.ConvertRecordTimeToOffsetResponse toProto() {
    StateServiceOuterClass.ConvertRecordTimeToOffsetResponse.Builder builder =
        StateServiceOuterClass.ConvertRecordTimeToOffsetResponse.newBuilder();
    builder.setOffset(offset);
    return builder.build();
  }
}
