// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

public final class HostedParty {
  private final @NonNull String party;
  private final @NonNull List<@NonNull HostingParticipant> hostingParticipants;

  public HostedParty(
      @NonNull String party, @NonNull List<@NonNull HostingParticipant> hostingParticipants) {
    this.party = party;
    this.hostingParticipants = List.copyOf(hostingParticipants);
  }

  @NonNull
  public String getParty() {
    return party;
  }

  @NonNull
  public List<@NonNull HostingParticipant> getHostingParticipants() {
    return hostingParticipants;
  }

  public static HostedParty fromProto(StateServiceOuterClass.HostedParty proto) {
    return new HostedParty(
        proto.getParty(),
        proto.getHostingParticipantsList().stream().map(HostingParticipant::fromProto).toList());
  }

  public StateServiceOuterClass.HostedParty toProto() {
    return StateServiceOuterClass.HostedParty.newBuilder()
        .setParty(party)
        .addAllHostingParticipants(
            hostingParticipants.stream().map(HostingParticipant::toProto).toList())
        .build();
  }

  @Override
  public String toString() {
    return "HostedParty{"
        + "party='"
        + party
        + '\''
        + ", hostingParticipants="
        + hostingParticipants
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostedParty that = (HostedParty) o;
    return Objects.equals(party, that.party)
        && Objects.equals(hostingParticipants, that.hostingParticipants);
  }

  @Override
  public int hashCode() {
    return Objects.hash(party, hostingParticipants);
  }
}
