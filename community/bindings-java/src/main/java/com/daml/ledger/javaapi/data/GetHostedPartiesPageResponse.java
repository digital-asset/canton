// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class GetHostedPartiesPageResponse {
  private final @NonNull List<@NonNull HostedParty> hostedParties;
  private final @NonNull Long hostedAtOffset;
  private final @NonNull Optional<ByteString> nextPageToken;

  public GetHostedPartiesPageResponse(
      @NonNull List<@NonNull HostedParty> hostedParties,
      @NonNull Long hostedAtOffset,
      @NonNull Optional<ByteString> nextPageToken) {
    this.hostedParties = List.copyOf(hostedParties);
    this.hostedAtOffset = hostedAtOffset;
    this.nextPageToken = nextPageToken;
  }

  @NonNull
  public List<@NonNull HostedParty> getHostedParties() {
    return hostedParties;
  }

  @NonNull
  public Long getHostedAtOffset() {
    return hostedAtOffset;
  }

  @NonNull
  public Optional<ByteString> getNextPageToken() {
    return nextPageToken;
  }

  public static GetHostedPartiesPageResponse fromProto(
      StateServiceOuterClass.GetHostedPartiesPageResponse response) {
    return new GetHostedPartiesPageResponse(
        response.getHostedPartiesList().stream().map(HostedParty::fromProto).toList(),
        response.getHostedAtOffset(),
        response.hasNextPageToken() ? Optional.of(response.getNextPageToken()) : Optional.empty());
  }

  public StateServiceOuterClass.GetHostedPartiesPageResponse toProto() {
    StateServiceOuterClass.GetHostedPartiesPageResponse.Builder builder =
        StateServiceOuterClass.GetHostedPartiesPageResponse.newBuilder()
            .addAllHostedParties(hostedParties.stream().map(HostedParty::toProto).toList())
            .setHostedAtOffset(hostedAtOffset);
    nextPageToken.ifPresent(builder::setNextPageToken);
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetHostedPartiesPageResponse{"
        + "hostedParties="
        + hostedParties
        + ", hostedAtOffset="
        + hostedAtOffset
        + ", nextPageToken="
        + nextPageToken
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetHostedPartiesPageResponse that = (GetHostedPartiesPageResponse) o;
    return Objects.equals(hostedParties, that.hostedParties)
        && Objects.equals(hostedAtOffset, that.hostedAtOffset)
        && Objects.equals(nextPageToken, that.nextPageToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostedParties, hostedAtOffset, nextPageToken);
  }
}
