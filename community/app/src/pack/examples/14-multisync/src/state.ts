// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// State queries: connected synchronizers.

import { components } from "../generated/api/ledger-api";
import { Client, valueOrError } from "./client";

export async function getConnectedSynchronizers(
    client: Client,
    party?: string,
): Promise<components["schemas"]["ConnectedSynchronizer"][]> {
    const resp = await client.GET("/v2/state/connected-synchronizers", {
        params: { query: { party } },
    });
    return valueOrError(resp).connectedSynchronizers ?? [];
}

