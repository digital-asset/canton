// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandsOuterClass;

import java.util.Objects;

public final class PrefetchContractKey {
  public final Identifier templateId;
  public final Value contractKey;
  public final int limit;

  public PrefetchContractKey(Identifier templateId, Value contractKey) {
    this(templateId, contractKey, 1);
  }

  public PrefetchContractKey(Identifier templateId, Value contractKey, int limit) {
    this.templateId = templateId;
    this.contractKey = contractKey;
    this.limit = limit;
  }

  public CommandsOuterClass.PrefetchContractKey toProto() {
    return CommandsOuterClass.PrefetchContractKey.newBuilder()
        .setTemplateId(this.templateId.toProto())
        .setContractKey(this.contractKey.toProto())
        .setLimit(this.limit)
        .build();
  }

  public static PrefetchContractKey fromProto(
      CommandsOuterClass.PrefetchContractKey prefetchContractKey) {
    Identifier templateId = Identifier.fromProto(prefetchContractKey.getTemplateId());
    Value contractKey = Value.fromProto(prefetchContractKey.getContractKey());
    int limit = prefetchContractKey.getLimit();
    if (limit <= 0) {
      if (limit == 0) {
        limit = 1;
      } else {
        limit = Integer.MAX_VALUE;
      }
    }
    return new PrefetchContractKey(templateId, contractKey, limit);
  }

  @Override
  public String toString() {
    return "PrefetchContractKey{"
        + "templateId="
        + templateId
        + ", contractKey='"
        + contractKey
        + "', limit="
        + limit
        + "'}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrefetchContractKey that = (PrefetchContractKey) o;
    return limit == that.limit
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(contractKey, that.contractKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateId, contractKey, limit);
  }
}
