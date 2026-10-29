// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public final class UnassignCommand extends ReassignmentCommand {

  private final @NonNull String contractId;

  private final @NonNull String source;

  private final @NonNull String target;

  private final @NonNull Optional<String> overrideSourceValidationPackageId;

  private final @NonNull Optional<String> overrideTargetValidationPackageId;

  public UnassignCommand(
      @NonNull String contractId,
      @NonNull String source,
      @NonNull String target,
      @NonNull Optional<String> overrideSourceValidationPackageId,
      @NonNull Optional<String> overrideTargetValidationPackageId) {
    this.contractId = contractId;
    this.source = source;
    this.target = target;
    this.overrideSourceValidationPackageId = overrideSourceValidationPackageId;
    this.overrideTargetValidationPackageId = overrideTargetValidationPackageId;
  }

  @NonNull
  public String getContractId() {
    return contractId;
  }

  @NonNull
  public String getSource() {
    return source;
  }

  @NonNull
  public String getTarget() {
    return target;
  }

  @NonNull
  public Optional<String> getOverrideSourceValidationPackageId() {
    return overrideSourceValidationPackageId;
  }

  @NonNull
  public Optional<String> getOverrideTargetValidationPackageId() {
    return overrideTargetValidationPackageId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UnassignCommand that = (UnassignCommand) o;
    return Objects.equals(contractId, that.contractId)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && Objects.equals(overrideSourceValidationPackageId, that.overrideSourceValidationPackageId)
        && Objects.equals(
            overrideTargetValidationPackageId, that.overrideTargetValidationPackageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        contractId,
        source,
        target,
        overrideSourceValidationPackageId,
        overrideTargetValidationPackageId);
  }

  @Override
  public String toString() {
    return "UnassignCommand{"
        + "contractId='"
        + contractId
        + '\''
        + ", source='"
        + source
        + '\''
        + ", target='"
        + target
        + '\''
        + ", overrideSourceValidationPackageId="
        + overrideSourceValidationPackageId
        + ", overrideTargetValidationPackageId="
        + overrideTargetValidationPackageId
        + '}';
  }

  public ReassignmentCommandOuterClass.UnassignCommand toProto() {

    ReassignmentCommandOuterClass.UnassignCommand.Builder builder =
        ReassignmentCommandOuterClass.UnassignCommand.newBuilder()
            .setContractId(this.contractId)
            .setSource(this.source)
            .setTarget(this.target);

    this.overrideSourceValidationPackageId.ifPresent(builder::setOverrideSourceValidationPackageId);
    this.overrideTargetValidationPackageId.ifPresent(builder::setOverrideTargetValidationPackageId);

    return builder.build();
  }

  public static UnassignCommand fromProto(
      ReassignmentCommandOuterClass.UnassignCommand unassignCommand) {

    Optional<String> overrideSourceValidationPackageId =
        unassignCommand.getOverrideSourceValidationPackageId().isEmpty()
            ? Optional.empty()
            : Optional.of(unassignCommand.getOverrideSourceValidationPackageId());

    Optional<String> overrideTargetValidationPackageId =
        unassignCommand.getOverrideTargetValidationPackageId().isEmpty()
            ? Optional.empty()
            : Optional.of(unassignCommand.getOverrideTargetValidationPackageId());

    return new UnassignCommand(
        unassignCommand.getContractId(),
        unassignCommand.getSource(),
        unassignCommand.getTarget(),
        overrideSourceValidationPackageId,
        overrideTargetValidationPackageId);
  }
}
