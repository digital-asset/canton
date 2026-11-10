// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Optional.empty;

import com.daml.ledger.javaapi.data.codegen.Update;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class can be used to build a valid submission for an Update. It provides {@link
 * #create(String, String, Update)} for initial creation and methods to set optional parameters e.g
 * {@link #withActAs(List)}, {@link #withWorkflowId(String)} etc.
 *
 * <p>Usage:
 *
 * <pre>
 *   var submission = UpdateSubmission.create(userId, commandId, update)
 *                                   .withAccessToken(token)
 *                                   .withParty(party)
 *                                   .with...
 * </pre>
 */
public final class UpdateSubmission<U> {
  @NonNull private final String userId;
  @NonNull private final String commandId;
  @NonNull private final Update<U> update;

  @NonNull private final Optional<String> workflowId;
  @NonNull private final List<@NonNull String> actAs;
  @NonNull private final List<@NonNull String> readAs;
  @NonNull private final Optional<Instant> minLedgerTimeAbs;
  @NonNull private final Optional<Duration> minLedgerTimeRel;
  @NonNull private final Optional<Duration> deduplicationDuration;
  @NonNull private final Optional<Long> deduplicationOffset;
  @NonNull private final Optional<String> accessToken;
  @NonNull private final List<DisclosedContract> disclosedContracts;
  @NonNull private final List<@NonNull String> packageIdSelectionPreference;
  @NonNull private List<@NonNull PrefetchContractKey> prefetchContractKeys;

  @NonNull private final Optional<String> synchronizerId;
  @NonNull private final Optional<String> accountId;

  private UpdateSubmission(
      @NonNull String userId,
      @NonNull String commandId,
      @NonNull Update<U> update,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<String> workflowId,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationDuration,
      @NonNull Optional<Long> deduplicationOffset,
      @NonNull Optional<String> accessToken,
      @NonNull Optional<String> synchronizerId,
      @NonNull List<DisclosedContract> disclosedContracts,
      @NonNull List<@NonNull String> packageIdSelectionPreference,
      @NonNull List<@NonNull PrefetchContractKey> prefetchContractKeys,
      @NonNull Optional<String> accountId) {
    this.workflowId = workflowId;
    this.userId = userId;
    this.commandId = commandId;
    this.actAs = actAs;
    this.readAs = readAs;
    this.minLedgerTimeAbs = minLedgerTimeAbs;
    this.minLedgerTimeRel = minLedgerTimeRel;
    this.deduplicationDuration = deduplicationDuration;
    this.deduplicationOffset = deduplicationOffset;
    this.update = update;
    this.accessToken = accessToken;
    this.synchronizerId = synchronizerId;
    this.disclosedContracts = disclosedContracts;
    this.packageIdSelectionPreference = packageIdSelectionPreference;
    this.prefetchContractKeys = prefetchContractKeys;
    this.accountId = accountId;
  }

  public static <U> UpdateSubmission<U> create(String userId, String commandId, Update<U> update) {
    return new UpdateSubmission<U>(
        userId,
        commandId,
        update,
        emptyList(),
        emptyList(),
        empty(),
        empty(),
        Optional.empty(),
        empty(),
        empty(),
        empty(),
        empty(),
        emptyList(),
        emptyList(),
        emptyList(),
        empty());
  }

  public Optional<String> getWorkflowId() {
    return workflowId;
  }

  public String getUserId() {
    return userId;
  }

  public String getCommandId() {
    return commandId;
  }

  public List<String> getActAs() {
    return unmodifiableList(actAs);
  }

  public List<String> getReadAs() {
    return unmodifiableList(readAs);
  }

  public Optional<Instant> getMinLedgerTimeAbs() {
    return minLedgerTimeAbs;
  }

  public Optional<Duration> getMinLedgerTimeRel() {
    return minLedgerTimeRel;
  }

  public Optional<Duration> getDeduplicationDuration() {
    return deduplicationDuration;
  }

  public Optional<Long> getDeduplicationOffset() {
    return deduplicationOffset;
  }

  public Update<U> getUpdate() {
    return update;
  }

  public Optional<String> getAccessToken() {
    return accessToken;
  }

  public Optional<String> getSynchronizerId() {
    return synchronizerId;
  }

  public @NonNull List<DisclosedContract> getDisclosedContracts() {
    return unmodifiableList(disclosedContracts);
  }

  public List<String> getPackageIdSelectionPreference() {
    return unmodifiableList(packageIdSelectionPreference);
  }

  public List<PrefetchContractKey> getPrefetchContractKeys() {
    return unmodifiableList(prefetchContractKeys);
  }

  public Optional<String> getAccountId() {
    return accountId;
  }

  private UpdateSubmission<U> copy(Consumer<Fields<U>> change) {
    Fields<U> fields = new Fields<>(this);
    change.accept(fields);
    return fields.build();
  }

  public UpdateSubmission<U> withWorkflowId(String workflowId) {
    return copy(f -> f.workflowId = Optional.of(workflowId));
  }

  public UpdateSubmission<U> withActAs(String actAs) {
    return copy(f -> f.actAs = List.of(actAs));
  }

  public UpdateSubmission<U> withActAs(List<@NonNull String> actAs) {
    return copy(f -> f.actAs = actAs);
  }

  public UpdateSubmission<U> withReadAs(List<@NonNull String> readAs) {
    return copy(f -> f.readAs = readAs);
  }

  public UpdateSubmission<U> withMinLedgerTimeAbs(Optional<Instant> minLedgerTimeAbs) {
    return copy(f -> f.minLedgerTimeAbs = minLedgerTimeAbs);
  }

  public UpdateSubmission<U> withMinLedgerTimeRel(Optional<Duration> minLedgerTimeRel) {
    return copy(f -> f.minLedgerTimeRel = minLedgerTimeRel);
  }

  public UpdateSubmission<U> withDeduplicationDuration(Optional<Duration> deduplicationDuration)
      throws CommandsSubmission.RedundantDeduplicationSpecification {
    if (deduplicationDuration.isPresent() && deduplicationOffset.isPresent()) {
      throw new CommandsSubmission.RedundantDeduplicationSpecification(
          deduplicationDuration.get(), deduplicationOffset.get());
    }
    return copy(f -> f.deduplicationDuration = deduplicationDuration);
  }

  public UpdateSubmission<U> withDeduplicationOffset(Optional<Long> deduplicationOffset)
      throws CommandsSubmission.RedundantDeduplicationSpecification {
    if (deduplicationOffset.isPresent() && deduplicationDuration.isPresent()) {
      throw new CommandsSubmission.RedundantDeduplicationSpecification(
          deduplicationDuration.get(), deduplicationOffset.get());
    }
    return copy(f -> f.deduplicationOffset = deduplicationOffset);
  }

  public UpdateSubmission<U> withAccessToken(Optional<String> accessToken) {
    return copy(f -> f.accessToken = accessToken);
  }

  public UpdateSubmission<U> withSynchronizerId(String synchronizerId) {
    return copy(f -> f.synchronizerId = Optional.of(synchronizerId));
  }

  public UpdateSubmission<U> withDisclosedContracts(
      List<@NonNull DisclosedContract> disclosedContracts) {
    return copy(f -> f.disclosedContracts = disclosedContracts);
  }

  public UpdateSubmission<U> withPackageIdSelectionPreference(
      List<@NonNull String> packageIdSelectionPreference) {
    return copy(f -> f.packageIdSelectionPreference = packageIdSelectionPreference);
  }

  public UpdateSubmission<U> withPrefetchContractKeys(
      List<PrefetchContractKey> prefetchContractKeys) {
    return copy(f -> f.prefetchContractKeys = prefetchContractKeys);
  }

  public UpdateSubmission<U> withAccountId(@NonNull String accountId) {
    return copy(f -> f.accountId = Utils.optionalNonEmpty(accountId));
  }

  public CommandsSubmission toCommandsSubmission() {
    return new CommandsSubmission(
        workflowId,
        userId,
        commandId,
        update.commands(),
        deduplicationDuration,
        deduplicationOffset,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        actAs,
        readAs,
        empty(),
        disclosedContracts,
        synchronizerId,
        accessToken,
        packageIdSelectionPreference,
        prefetchContractKeys,
        accountId);
  }

  private static final class Fields<U> {
    String userId;
    String commandId;
    Update<U> update;
    List<String> actAs;
    List<String> readAs;
    Optional<String> workflowId;
    Optional<Instant> minLedgerTimeAbs;
    Optional<Duration> minLedgerTimeRel;
    Optional<Duration> deduplicationDuration;
    Optional<Long> deduplicationOffset;
    Optional<String> accessToken;
    Optional<String> synchronizerId;
    List<DisclosedContract> disclosedContracts;
    List<String> packageIdSelectionPreference;
    List<PrefetchContractKey> prefetchContractKeys;
    Optional<String> accountId;

    Fields(UpdateSubmission<U> s) {
      userId = s.userId;
      commandId = s.commandId;
      update = s.update;
      actAs = s.actAs;
      readAs = s.readAs;
      workflowId = s.workflowId;
      minLedgerTimeAbs = s.minLedgerTimeAbs;
      minLedgerTimeRel = s.minLedgerTimeRel;
      deduplicationDuration = s.deduplicationDuration;
      deduplicationOffset = s.deduplicationOffset;
      accessToken = s.accessToken;
      synchronizerId = s.synchronizerId;
      disclosedContracts = s.disclosedContracts;
      packageIdSelectionPreference = s.packageIdSelectionPreference;
      prefetchContractKeys = s.prefetchContractKeys;
      accountId = s.accountId;
    }

    UpdateSubmission<U> build() {
      return new UpdateSubmission<>(
          userId,
          commandId,
          update,
          actAs,
          readAs,
          workflowId,
          minLedgerTimeAbs,
          minLedgerTimeRel,
          deduplicationDuration,
          deduplicationOffset,
          accessToken,
          synchronizerId,
          disclosedContracts,
          packageIdSelectionPreference,
          prefetchContractKeys,
          accountId);
    }
  }
}
