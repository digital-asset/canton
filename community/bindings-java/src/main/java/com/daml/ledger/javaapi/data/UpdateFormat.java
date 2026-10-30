// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class UpdateFormat {

  @NonNull private final Optional<@NonNull TransactionFormat> includeTransactions;
  @NonNull private final Optional<@NonNull EventFormat> includeReassignments;
  @NonNull private final Optional<@NonNull TopologyFormat> includeTopologyEvents;

  @NonNull private final Optional<@NonNull TransactionFormat> includeTransactionsAndReassignments;

  public static UpdateFormat fromProto(TransactionFilterOuterClass.UpdateFormat protoFormat) {
    return new UpdateFormat(
        protoFormat.hasIncludeTransactions()
            ? Optional.of(TransactionFormat.fromProto(protoFormat.getIncludeTransactions()))
            : Optional.empty(),
        protoFormat.hasIncludeReassignments()
            ? Optional.of(EventFormat.fromProto(protoFormat.getIncludeReassignments()))
            : Optional.empty(),
        protoFormat.hasIncludeTopologyEvents()
            ? Optional.of(TopologyFormat.fromProto(protoFormat.getIncludeTopologyEvents()))
            : Optional.empty(),
        protoFormat.hasIncludeTransactionsAndReassignments()
            ? Optional.of(
                TransactionFormat.fromProto(protoFormat.getIncludeTransactionsAndReassignments()))
            : Optional.empty());
  }

  public TransactionFilterOuterClass.UpdateFormat toProto() {
    TransactionFilterOuterClass.UpdateFormat.Builder builder =
        TransactionFilterOuterClass.UpdateFormat.newBuilder();
    includeTransactions.ifPresent(
        transactionFormat -> builder.setIncludeTransactions(transactionFormat.toProto()));
    includeReassignments.ifPresent(
        eventFormat -> builder.setIncludeReassignments(eventFormat.toProto()));
    includeTopologyEvents.ifPresent(
        topologyFormat -> builder.setIncludeTopologyEvents(topologyFormat.toProto()));
    includeTransactionsAndReassignments.ifPresent(
        transactionFormat ->
            builder.setIncludeTransactionsAndReassignments(transactionFormat.toProto()));
    return builder.build();
  }

  public Optional<@NonNull TransactionFormat> getIncludeTransactions() {
    return includeTransactions;
  }

  public Optional<@NonNull EventFormat> getIncludeReassignments() {
    return includeReassignments;
  }

  public Optional<@NonNull TopologyFormat> getIncludeTopologyEvents() {
    return includeTopologyEvents;
  }

  /**
   * Shorthand for including both transactions in the given format and (un)assignments filtered by
   * its event format. If set, {@link #getIncludeTransactions()} and {@link
   * #getIncludeReassignments()} must be empty.
   */
  public Optional<@NonNull TransactionFormat> getIncludeTransactionsAndReassignments() {
    return includeTransactionsAndReassignments;
  }

  public UpdateFormat(
      @NonNull Optional<@NonNull TransactionFormat> includeTransactions,
      @NonNull Optional<@NonNull EventFormat> includeReassignments,
      @NonNull Optional<@NonNull TopologyFormat> includeTopologyEvents) {
    this(includeTransactions, includeReassignments, includeTopologyEvents, Optional.empty());
  }

  public UpdateFormat(
      @NonNull Optional<@NonNull TransactionFormat> includeTransactions,
      @NonNull Optional<@NonNull EventFormat> includeReassignments,
      @NonNull Optional<@NonNull TopologyFormat> includeTopologyEvents,
      @NonNull Optional<@NonNull TransactionFormat> includeTransactionsAndReassignments) {
    this.includeTransactions = includeTransactions;
    this.includeReassignments = includeReassignments;
    this.includeTopologyEvents = includeTopologyEvents;
    this.includeTransactionsAndReassignments = includeTransactionsAndReassignments;
  }

  @Override
  public String toString() {
    return "UpdateFormat{"
        + "includeTransactions="
        + includeTransactions
        + ", includeReassignments="
        + includeReassignments
        + ", includeTopologyEvents="
        + includeTopologyEvents
        + ", includeTransactionsAndReassignments="
        + includeTransactionsAndReassignments
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UpdateFormat that = (UpdateFormat) o;
    return Objects.equals(includeTransactions, that.includeTransactions)
        && Objects.equals(includeReassignments, that.includeReassignments)
        && Objects.equals(includeTopologyEvents, that.includeTopologyEvents)
        && Objects.equals(
            includeTransactionsAndReassignments, that.includeTransactionsAndReassignments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        includeTransactions,
        includeReassignments,
        includeTopologyEvents,
        includeTransactionsAndReassignments);
  }
}
