// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

public final class GetLedgerEndRequest {

  @NonNull private final List<@NonNull String> synchronizerIds;

  public GetLedgerEndRequest(@NonNull List<@NonNull String> synchronizerIds) {
    this.synchronizerIds = synchronizerIds;
  }

  @NonNull
  public List<@NonNull String> getSynchronizerIds() {
    return synchronizerIds;
  }

  public static GetLedgerEndRequest fromProto(StateServiceOuterClass.GetLedgerEndRequest request) {
    return new GetLedgerEndRequest(request.getSynchronizerIdList());
  }

  public StateServiceOuterClass.GetLedgerEndRequest toProto() {
    return StateServiceOuterClass.GetLedgerEndRequest.newBuilder()
        .addAllSynchronizerId(this.synchronizerIds)
        .build();
  }

  @Override
  public String toString() {
    return "GetLedgerEndRequest{" + "synchronizerIds=" + synchronizerIds + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    GetLedgerEndRequest that = (GetLedgerEndRequest) o;
    return Objects.equals(synchronizerIds, that.synchronizerIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(synchronizerIds);
  }
}
