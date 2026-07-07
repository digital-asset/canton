// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

public final class GetCompletionsRequest {

  @NonNull private final List<@NonNull String> parties;

  @NonNull private final Long beginExclusive;

  public GetCompletionsRequest(
      @NonNull List<@NonNull String> parties, @NonNull Long beginExclusive) {
    this.parties = List.copyOf(parties);
    this.beginExclusive = beginExclusive;
  }

  @NonNull
  public List<@NonNull String> getParties() {
    return parties;
  }

  public Long getBeginExclusive() {
    return beginExclusive;
  }

  public static GetCompletionsRequest fromProto(
      CommandCompletionServiceOuterClass.GetCompletionsRequest request) {
    return new GetCompletionsRequest(request.getPartiesList(), request.getBeginExclusive());
  }

  public CommandCompletionServiceOuterClass.GetCompletionsRequest toProto() {
    return CommandCompletionServiceOuterClass.GetCompletionsRequest.newBuilder()
        .addAllParties(parties)
        .setBeginExclusive(beginExclusive)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetCompletionsRequest that = (GetCompletionsRequest) o;
    return Objects.equals(parties, that.parties)
        && Objects.equals(beginExclusive, that.beginExclusive);
  }

  @Override
  public int hashCode() {

    return Objects.hash(parties, beginExclusive);
  }

  @Override
  public String toString() {
    return "GetCompletionsRequest{"
        + "parties="
        + parties
        + ", beginExclusive="
        + beginExclusive
        + '}';
  }
}
