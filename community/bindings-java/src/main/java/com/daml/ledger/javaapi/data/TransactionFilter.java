// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class TransactionFilter {

  public static TransactionFilter fromProto(
      TransactionFilterOuterClass.TransactionFilter transactionFilter) {
    // at the moment, the only transaction filter supported is FiltersByParty
    return FiltersByParty.fromProto(transactionFilter);
  }

  public abstract TransactionFilterOuterClass.TransactionFilter toProto();

  public abstract Set<String> getParties();

  public static TransactionFilter transactionFilter(
      ContractTypeCompanion<?, ?, ?, ?> contractCompanion, Set<String> parties) {
    Filter filter =
        (contractCompanion instanceof ContractCompanion)
            ? new InclusiveFilter(
                Collections.emptyMap(),
                Collections.singletonMap(
                    contractCompanion.TEMPLATE_ID, Filter.Template.HIDE_CREATED_EVENT_BLOB))
            : new InclusiveFilter(
                Map.of(
                    contractCompanion.TEMPLATE_ID,
                    Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB),
                Collections.emptyMap());
    Map<String, Filter> partyToFilters =
        parties.stream().collect(Collectors.toMap(Function.identity(), x -> filter));
    return new FiltersByParty(partyToFilters);
  }
}
