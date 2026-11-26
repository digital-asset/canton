// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;

public enum HostingState {
  ONBOARDING,
  ACTIVE;

  public StateServiceOuterClass.HostingState toProto() {
    return this == ONBOARDING
        ? StateServiceOuterClass.HostingState.HOSTING_STATE_ONBOARDING
        : StateServiceOuterClass.HostingState.HOSTING_STATE_ACTIVE;
  }

  public static HostingState fromProto(StateServiceOuterClass.HostingState proto) {
    if (proto == StateServiceOuterClass.HostingState.HOSTING_STATE_ONBOARDING) return ONBOARDING;
    if (proto == StateServiceOuterClass.HostingState.HOSTING_STATE_ACTIVE) return ACTIVE;
    throw new IllegalArgumentException(
        "Proto HostingState " + proto + " cannot be converted to javaapi.data.HostingState");
  }
}
