// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Filter matching events of a specific contract id. Corresponds to the <code>ContractIdFilter
 * </code> proto message.
 */
public final class ContractIdFilter {

  private final String contractId;
  private final boolean includeCreatedEventBlob;
  private final List<Identifier> includeInterfaceViews;

  public ContractIdFilter(
      @NonNull String contractId,
      boolean includeCreatedEventBlob,
      @NonNull List<@NonNull Identifier> includeInterfaceViews) {
    this.contractId = contractId;
    this.includeCreatedEventBlob = includeCreatedEventBlob;
    this.includeInterfaceViews = List.copyOf(includeInterfaceViews);
  }

  public static ContractIdFilter fromProto(TransactionFilterOuterClass.ContractIdFilter proto) {
    return new ContractIdFilter(
        proto.getContractId(),
        proto.getIncludeCreatedEventBlob(),
        proto.getIncludeInterfaceViewsList().stream()
            .map(Identifier::fromProto)
            .collect(Collectors.toList()));
  }

  public TransactionFilterOuterClass.ContractIdFilter toProto() {
    return TransactionFilterOuterClass.ContractIdFilter.newBuilder()
        .setContractId(contractId)
        .setIncludeCreatedEventBlob(includeCreatedEventBlob)
        .addAllIncludeInterfaceViews(
            includeInterfaceViews.stream().map(Identifier::toProto).collect(Collectors.toList()))
        .build();
  }

  @NonNull
  public String getContractId() {
    return contractId;
  }

  public boolean getIncludeCreatedEventBlob() {
    return includeCreatedEventBlob;
  }

  @NonNull
  public List<@NonNull Identifier> getIncludeInterfaceViews() {
    return includeInterfaceViews;
  }

  @Override
  public String toString() {
    return "ContractIdFilter{"
        + "contractId='"
        + contractId
        + '\''
        + ", includeCreatedEventBlob="
        + includeCreatedEventBlob
        + ", includeInterfaceViews="
        + includeInterfaceViews
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ContractIdFilter that = (ContractIdFilter) o;
    return includeCreatedEventBlob == that.includeCreatedEventBlob
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(includeInterfaceViews, that.includeInterfaceViews);
  }

  @Override
  public int hashCode() {
    return Objects.hash(contractId, includeCreatedEventBlob, includeInterfaceViews);
  }
}
