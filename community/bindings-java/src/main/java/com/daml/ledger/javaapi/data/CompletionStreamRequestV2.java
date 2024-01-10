// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class CompletionStreamRequestV2 {

  @NonNull private final String applicationId;

  @NonNull private final List<@NonNull String> parties;

  @NonNull private final ParticipantOffsetV2 beginExclusive;

  public CompletionStreamRequestV2(
      @NonNull String applicationId,
      @NonNull List<@NonNull String> parties,
      @NonNull ParticipantOffsetV2 beginExclusive) {
    this.applicationId = applicationId;
    this.parties = List.copyOf(parties);
    this.beginExclusive = beginExclusive;
  }

  @NonNull
  public String getApplicationId() {
    return applicationId;
  }

  @NonNull
  public List<@NonNull String> getParties() {
    return parties;
  }

  public ParticipantOffsetV2 getBeginExclusive() {
    return beginExclusive;
  }

  public static CompletionStreamRequestV2 fromProto(
      CommandCompletionServiceOuterClass.CompletionStreamRequest request) {
    return new CompletionStreamRequestV2(
        request.getApplicationId(),
        request.getPartiesList(),
        ParticipantOffsetV2.fromProto(request.getBeginExclusive()));
  }

  public CommandCompletionServiceOuterClass.CompletionStreamRequest toProto() {
    return CommandCompletionServiceOuterClass.CompletionStreamRequest.newBuilder()
        .setApplicationId(applicationId)
        .addAllParties(parties)
        .setBeginExclusive(beginExclusive.toProto())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompletionStreamRequestV2 that = (CompletionStreamRequestV2) o;
    return Objects.equals(applicationId, that.applicationId)
        && Objects.equals(parties, that.parties)
        && Objects.equals(beginExclusive, that.beginExclusive);
  }

  @Override
  public int hashCode() {

    return Objects.hash(applicationId, parties, beginExclusive);
  }

  @Override
  public String toString() {
    return "CompletionStreamRequest{"
        + "applicationId="
        + applicationId
        + ", parties="
        + parties
        + ", beginExclusive="
        + beginExclusive
        + '}';
  }
}
