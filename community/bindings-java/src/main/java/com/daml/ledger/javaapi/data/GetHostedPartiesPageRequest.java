// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetHostedPartiesPageRequest {
  private final @NonNull Optional<Long> hostedAtOffset;
  private final @NonNull Optional<HostedPartiesFilter> filter;
  private final @NonNull Optional<ByteString> pageToken;
  private final @NonNull Optional<Integer> maxPageSize;

  public GetHostedPartiesPageRequest(
      @NonNull Optional<Long> hostedAtOffset,
      @NonNull Optional<HostedPartiesFilter> filter,
      @NonNull Optional<ByteString> pageToken,
      @NonNull Optional<Integer> maxPageSize) {
    this.hostedAtOffset = hostedAtOffset;
    this.filter = filter;
    this.pageToken = pageToken;
    this.maxPageSize = maxPageSize;
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
  public Optional<ByteString> getPageToken() {
    return pageToken;
  }

  @NonNull
  public Optional<Integer> getMaxPageSize() {
    return maxPageSize;
  }

  public static GetHostedPartiesPageRequest fromProto(
      StateServiceOuterClass.GetHostedPartiesPageRequest request) {
    return new GetHostedPartiesPageRequest(
        request.hasHostedAtOffset() ? Optional.of(request.getHostedAtOffset()) : Optional.empty(),
        request.hasFilter()
            ? Optional.of(HostedPartiesFilter.fromProto(request.getFilter()))
            : Optional.empty(),
        request.hasPageToken() ? Optional.of(request.getPageToken()) : Optional.empty(),
        request.hasMaxPageSize() ? Optional.of(request.getMaxPageSize()) : Optional.empty());
  }

  public StateServiceOuterClass.GetHostedPartiesPageRequest toProto() {
    StateServiceOuterClass.GetHostedPartiesPageRequest.Builder builder =
        StateServiceOuterClass.GetHostedPartiesPageRequest.newBuilder();
    hostedAtOffset.ifPresent(builder::setHostedAtOffset);
    filter.ifPresent(f -> builder.setFilter(f.toProto()));
    pageToken.ifPresent(builder::setPageToken);
    maxPageSize.ifPresent(builder::setMaxPageSize);
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetHostedPartiesPageRequest{"
        + "hostedAtOffset="
        + hostedAtOffset
        + ", filter="
        + filter
        + ", pageToken="
        + pageToken
        + ", maxPageSize="
        + maxPageSize
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetHostedPartiesPageRequest that = (GetHostedPartiesPageRequest) o;
    return Objects.equals(hostedAtOffset, that.hostedAtOffset)
        && Objects.equals(filter, that.filter)
        && Objects.equals(pageToken, that.pageToken)
        && Objects.equals(maxPageSize, that.maxPageSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostedAtOffset, filter, pageToken, maxPageSize);
  }
}
