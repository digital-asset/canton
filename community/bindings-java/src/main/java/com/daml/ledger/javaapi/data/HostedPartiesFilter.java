// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

public final class HostedPartiesFilter {
  private final @NonNull List<@NonNull String> partyFilter;
  private final boolean allParticipants;
  private final @NonNull List<@NonNull String> synchronizerIdFilter;

  public HostedPartiesFilter(
      @NonNull List<@NonNull String> partyFilter,
      boolean allParticipants,
      @NonNull List<@NonNull String> synchronizerIdFilter) {
    this.partyFilter = List.copyOf(partyFilter);
    this.allParticipants = allParticipants;
    this.synchronizerIdFilter = List.copyOf(synchronizerIdFilter);
  }

  @NonNull
  public List<@NonNull String> getPartyFilter() {
    return partyFilter;
  }

  public boolean isAllParticipants() {
    return allParticipants;
  }

  @NonNull
  public List<@NonNull String> getSynchronizerIdFilter() {
    return synchronizerIdFilter;
  }

  public static HostedPartiesFilter fromProto(StateServiceOuterClass.HostedPartiesFilter proto) {
    return new HostedPartiesFilter(
        proto.getPartyFilterList(),
        proto.getAllParticipants(),
        proto.getSynchronizerIdFilterList());
  }

  public StateServiceOuterClass.HostedPartiesFilter toProto() {
    return StateServiceOuterClass.HostedPartiesFilter.newBuilder()
        .addAllPartyFilter(partyFilter)
        .setAllParticipants(allParticipants)
        .addAllSynchronizerIdFilter(synchronizerIdFilter)
        .build();
  }

  @Override
  public String toString() {
    return "HostedPartiesFilter{"
        + "partyFilter="
        + partyFilter
        + ", allParticipants="
        + allParticipants
        + ", synchronizerIdFilter="
        + synchronizerIdFilter
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostedPartiesFilter that = (HostedPartiesFilter) o;
    return allParticipants == that.allParticipants
        && Objects.equals(partyFilter, that.partyFilter)
        && Objects.equals(synchronizerIdFilter, that.synchronizerIdFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partyFilter, allParticipants, synchronizerIdFilter);
  }
}
