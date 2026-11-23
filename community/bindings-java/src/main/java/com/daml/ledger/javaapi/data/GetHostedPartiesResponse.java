// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetHostedPartiesResponse {
  private final @NonNull HostedParty hostedParty;
  private final @NonNull Long hostedAtOffset;
  private final @NonNull ByteString streamContinuationToken;

  public GetHostedPartiesResponse(
      @NonNull HostedParty hostedParty,
      @NonNull Long hostedAtOffset,
      @NonNull ByteString streamContinuationToken) {
    this.hostedParty = hostedParty;
    this.hostedAtOffset = hostedAtOffset;
    this.streamContinuationToken = streamContinuationToken;
  }

  @NonNull
  public HostedParty getHostedParty() {
    return hostedParty;
  }

  @NonNull
  public Long getHostedAtOffset() {
    return hostedAtOffset;
  }

  @NonNull
  public ByteString getStreamContinuationToken() {
    return streamContinuationToken;
  }

  public static GetHostedPartiesResponse fromProto(
      StateServiceOuterClass.GetHostedPartiesResponse response) {
    if (!response.hasHostedParty()) {
      throw new IllegalArgumentException("Response has no hostedParty defined.");
    }
    return new GetHostedPartiesResponse(
        HostedParty.fromProto(response.getHostedParty()),
        response.getHostedAtOffset(),
        response.getStreamContinuationToken());
  }

  public StateServiceOuterClass.GetHostedPartiesResponse toProto() {
    return StateServiceOuterClass.GetHostedPartiesResponse.newBuilder()
        .setHostedParty(hostedParty.toProto())
        .setHostedAtOffset(hostedAtOffset)
        .setStreamContinuationToken(streamContinuationToken)
        .build();
  }

  @Override
  public String toString() {
    return "GetHostedPartiesResponse{"
        + "hostedParty="
        + hostedParty
        + ", hostedAtOffset="
        + hostedAtOffset
        + ", streamContinuationToken="
        + streamContinuationToken
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetHostedPartiesResponse that = (GetHostedPartiesResponse) o;
    return Objects.equals(hostedParty, that.hostedParty)
        && Objects.equals(hostedAtOffset, that.hostedAtOffset)
        && Objects.equals(streamContinuationToken, that.streamContinuationToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostedParty, hostedAtOffset, streamContinuationToken);
  }
}
