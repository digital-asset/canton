// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public final class GetCompletionByHashResponse {

  @NonNull private final Optional<Completion> acceptedCompletion;

  @NonNull private final List<@NonNull Completion> lastRejectedCompletions;

  public GetCompletionByHashResponse(
      @NonNull Optional<Completion> acceptedCompletion,
      @NonNull List<@NonNull Completion> lastRejectedCompletions) {
    this.acceptedCompletion = acceptedCompletion;
    this.lastRejectedCompletions = Collections.unmodifiableList(lastRejectedCompletions);
  }

  @NonNull
  public Optional<Completion> getAcceptedCompletion() {
    return acceptedCompletion;
  }

  @NonNull
  public List<@NonNull Completion> getLastRejectedCompletions() {
    return lastRejectedCompletions;
  }

  public static GetCompletionByHashResponse fromProto(
      CommandCompletionServiceOuterClass.GetCompletionByHashResponse response) {
    Optional<Completion> acceptedCompletion =
        response.hasAcceptedCompletion()
            ? Optional.of(Completion.fromProto(response.getAcceptedCompletion()))
            : Optional.empty();
    List<Completion> lastRejectedCompletions =
        response.getLastRejectedCompletionsList().stream()
            .map(Completion::fromProto)
            .collect(Collectors.toList());
    return new GetCompletionByHashResponse(acceptedCompletion, lastRejectedCompletions);
  }

  public CommandCompletionServiceOuterClass.GetCompletionByHashResponse toProto() {
    CommandCompletionServiceOuterClass.GetCompletionByHashResponse.Builder builder =
        CommandCompletionServiceOuterClass.GetCompletionByHashResponse.newBuilder();
    acceptedCompletion.ifPresent(completion -> builder.setAcceptedCompletion(completion.toProto()));
    builder.addAllLastRejectedCompletions(
        lastRejectedCompletions.stream().map(Completion::toProto).collect(Collectors.toList()));
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetCompletionByHashResponse that = (GetCompletionByHashResponse) o;
    return Objects.equals(acceptedCompletion, that.acceptedCompletion)
        && Objects.equals(lastRejectedCompletions, that.lastRejectedCompletions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(acceptedCompletion, lastRejectedCompletions);
  }

  @Override
  public String toString() {
    return "GetCompletionByHashResponse{"
        + "acceptedCompletion="
        + acceptedCompletion
        + ", lastRejectedCompletions="
        + lastRejectedCompletions
        + '}';
  }
}
