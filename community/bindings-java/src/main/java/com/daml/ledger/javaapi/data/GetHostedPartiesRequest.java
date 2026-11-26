// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetHostedPartiesRequest {
  private final @NonNull Optional<Long> hostedAtOffset;
  private final @NonNull Optional<HostedPartiesFilter> filter;
  private final @NonNull Optional<ByteString> streamContinuationToken;

  public GetHostedPartiesRequest(
      @NonNull Optional<Long> hostedAtOffset,
      @NonNull Optional<HostedPartiesFilter> filter,
      @NonNull Optional<ByteString> streamContinuationToken) {
    this.hostedAtOffset = hostedAtOffset;
    this.filter = filter;
    this.streamContinuationToken = streamContinuationToken;
  }

  @NonNull
  public Optional<Long> getHostedAtOffset() {
    return hostedAtOffset;
  }

  @NonNull
  public Optional<HostedPartiesFilter> getFilter() {
    return filter;
  }

  @NonNull
  public Optional<ByteString> getStreamContinuationToken() {
    return streamContinuationToken;
  }

  public static GetHostedPartiesRequest fromProto(
      StateServiceOuterClass.GetHostedPartiesRequest request) {
    return new GetHostedPartiesRequest(
        request.hasHostedAtOffset() ? Optional.of(request.getHostedAtOffset()) : Optional.empty(),
        request.hasFilter()
            ? Optional.of(HostedPartiesFilter.fromProto(request.getFilter()))
            : Optional.empty(),
        request.hasStreamContinuationToken()
            ? Optional.of(request.getStreamContinuationToken())
            : Optional.empty());
  }

  public StateServiceOuterClass.GetHostedPartiesRequest toProto() {
    StateServiceOuterClass.GetHostedPartiesRequest.Builder builder =
        StateServiceOuterClass.GetHostedPartiesRequest.newBuilder();
    hostedAtOffset.ifPresent(builder::setHostedAtOffset);
    filter.ifPresent(f -> builder.setFilter(f.toProto()));
    streamContinuationToken.ifPresent(builder::setStreamContinuationToken);
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetHostedPartiesRequest{"
        + "hostedAtOffset="
        + hostedAtOffset
        + ", filter="
        + filter
        + ", streamContinuationToken="
        + streamContinuationToken
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetHostedPartiesRequest that = (GetHostedPartiesRequest) o;
    return Objects.equals(hostedAtOffset, that.hostedAtOffset)
        && Objects.equals(filter, that.filter)
        && Objects.equals(streamContinuationToken, that.streamContinuationToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostedAtOffset, filter, streamContinuationToken);
  }
}
