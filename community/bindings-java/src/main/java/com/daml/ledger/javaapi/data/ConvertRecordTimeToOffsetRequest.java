// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public class ConvertRecordTimeToOffsetRequest {
  @NonNull private final Instant recordTime;
  @NonNull private final String synchronizerId;

  public ConvertRecordTimeToOffsetRequest(
      @NonNull Instant recordTime, @NonNull String synchronizerId) {
    this.recordTime = recordTime;
    this.synchronizerId = synchronizerId;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    ConvertRecordTimeToOffsetRequest that = (ConvertRecordTimeToOffsetRequest) o;
    return Objects.equals(recordTime, that.recordTime)
        && Objects.equals(synchronizerId, that.synchronizerId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordTime, synchronizerId);
  }

  @Override
  public String toString() {
    return "ConvertRecordTimeToOffsetRequest{"
        + "recordTime="
        + recordTime
        + ", synchronizerId='"
        + synchronizerId
        + '\''
        + '}';
  }

  public static ConvertRecordTimeToOffsetRequest fromProto(
      StateServiceOuterClass.ConvertRecordTimeToOffsetRequest request) {
    if (request.hasRecordTime()) {
      return new ConvertRecordTimeToOffsetRequest(
          Utils.instantFromProto(request.getRecordTime()), request.getSynchronizerId());
    } else {
      throw new IllegalArgumentException("Request has no record time defined");
    }
  }

  public StateServiceOuterClass.ConvertRecordTimeToOffsetRequest toProto() {
    StateServiceOuterClass.ConvertRecordTimeToOffsetRequest.Builder builder =
        StateServiceOuterClass.ConvertRecordTimeToOffsetRequest.newBuilder();
    return builder
        .setRecordTime(Utils.instantToProto(recordTime))
        .setSynchronizerId(synchronizerId)
        .build();
  }
}
