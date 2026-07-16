// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetCompletionByHashRequest {

  @NonNull private final ByteString transactionHash;

  public GetCompletionByHashRequest(@NonNull ByteString transactionHash) {
    this.transactionHash = transactionHash;
  }

  @NonNull
  public ByteString getTransactionHash() {
    return transactionHash;
  }

  public static GetCompletionByHashRequest fromProto(
      CommandCompletionServiceOuterClass.GetCompletionByHashRequest request) {
    return new GetCompletionByHashRequest(request.getTransactionHash());
  }

  public CommandCompletionServiceOuterClass.GetCompletionByHashRequest toProto() {
    return CommandCompletionServiceOuterClass.GetCompletionByHashRequest.newBuilder()
        .setTransactionHash(transactionHash)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetCompletionByHashRequest that = (GetCompletionByHashRequest) o;
    return Objects.equals(transactionHash, that.transactionHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionHash);
  }

  @Override
  public String toString() {
    return "GetCompletionByHashRequest{" + "transactionHash=" + transactionHash + '}';
  }
}
