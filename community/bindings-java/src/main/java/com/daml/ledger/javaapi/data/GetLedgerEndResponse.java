// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public final class GetLedgerEndResponse {

  @NonNull private final Long offset;
  @NonNull private final List<@NonNull SynchronizerTime> synchronizerTimes;

  public GetLedgerEndResponse(
      @NonNull Long offset, @NonNull List<@NonNull SynchronizerTime> synchronizerTimes) {
    this.offset = offset;
    this.synchronizerTimes = synchronizerTimes;
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  public static GetLedgerEndResponse fromProto(
      StateServiceOuterClass.GetLedgerEndResponse response) {
    return new GetLedgerEndResponse(
        response.getOffset(),
        response.getSynchronizerTimesList().stream()
            .map(SynchronizerTime::fromProto)
            .collect(Collectors.toList()));
  }

  public @NonNull List<@NonNull SynchronizerTime> getSynchronizerTimes() {
    return synchronizerTimes;
  }

  public StateServiceOuterClass.GetLedgerEndResponse toProto() {
    return StateServiceOuterClass.GetLedgerEndResponse.newBuilder()
        .setOffset(this.offset)
        .addAllSynchronizerTimes(
            this.synchronizerTimes.stream().map(SynchronizerTime::toProto).toList())
        .build();
  }

  @Override
  public String toString() {
    return "GetLedgerEndResponse{"
        + "offset="
        + offset
        + ", synchronizerTimes="
        + synchronizerTimes
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    GetLedgerEndResponse that = (GetLedgerEndResponse) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(synchronizerTimes, that.synchronizerTimes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, synchronizerTimes);
  }
}
