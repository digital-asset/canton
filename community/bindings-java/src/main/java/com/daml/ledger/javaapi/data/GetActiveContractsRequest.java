// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetActiveContractsRequest {

  @NonNull private final EventFormat eventFormat;

  @NonNull private final Long activeAtOffset;

  private final boolean includeIncompleteReassignments;

  public GetActiveContractsRequest(@NonNull EventFormat eventFormat, @NonNull Long activeAtOffset) {
    this(eventFormat, activeAtOffset, false);
  }

  public GetActiveContractsRequest(
      @NonNull EventFormat eventFormat,
      @NonNull Long activeAtOffset,
      boolean includeIncompleteReassignments) {
    this.eventFormat = eventFormat;
    this.activeAtOffset = activeAtOffset;
    this.includeIncompleteReassignments = includeIncompleteReassignments;
  }

  public static GetActiveContractsRequest fromProto(
      StateServiceOuterClass.GetActiveContractsRequest request) {
    if (request.hasEventFormat()) {
      return new GetActiveContractsRequest(
          EventFormat.fromProto(request.getEventFormat()),
          request.getActiveAtOffset(),
          request.getIncludeIncompleteReassignments());
    } else {
      throw new IllegalArgumentException("Request has no eventFormat defined.");
    }
  }

  public StateServiceOuterClass.GetActiveContractsRequest toProto() {
    return StateServiceOuterClass.GetActiveContractsRequest.newBuilder()
        .setEventFormat(this.eventFormat.toProto())
        .setActiveAtOffset(this.activeAtOffset)
        .setIncludeIncompleteReassignments(this.includeIncompleteReassignments)
        .build();
  }

  @NonNull
  public Long getActiveAtOffset() {
    return activeAtOffset;
  }

  public boolean getIncludeIncompleteReassignments() {
    return includeIncompleteReassignments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetActiveContractsRequest that = (GetActiveContractsRequest) o;
    return includeIncompleteReassignments == that.includeIncompleteReassignments
        && Objects.equals(eventFormat, that.eventFormat)
        && Objects.equals(activeAtOffset, that.activeAtOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventFormat, activeAtOffset, includeIncompleteReassignments);
  }

  @Override
  public String toString() {
    return "GetActiveContractsRequest{"
        + "eventFormat="
        + eventFormat
        + ", activeAtOffset='"
        + activeAtOffset
        + '\''
        + ", includeIncompleteReassignments="
        + includeIncompleteReassignments
        + '}';
  }
}
