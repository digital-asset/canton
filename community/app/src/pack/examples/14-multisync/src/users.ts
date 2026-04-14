// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// User management: create users and define rights.

import { components } from "../generated/api/ledger-api";
import { Client } from "./client";

export async function createUser(
    client: Client,
    userId: string,
    primaryParty: string,
    rights: components["schemas"]["Right"][],
): Promise<void> {
    await client.POST("/v2/users", {
        body: {
            user: { id: userId, primaryParty, isDeactivated: false, identityProviderId: "" },
            rights,
        },
    });
}

export function makeCanActAs(party: string): components["schemas"]["Right"] {
    return { kind: { CanActAs: { value: { party } } } };
}

export function makeCanReadAs(party: string): components["schemas"]["Right"] {
    return { kind: { CanReadAs: { value: { party } } } };
}

