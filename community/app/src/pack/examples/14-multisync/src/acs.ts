// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Active contract set (ACS) queries and helpers.

import {components} from "../generated/api/ledger-api";
import {Client, valueOrError} from "./client";

export interface ActiveContract {
    contractId: string;
    templateId: string;
    synchronizerId: string;
    createArgument: Record<string, unknown>;
    createdEventBlob?: string;
}

/**
 * Query active contracts visible to a party at a given offset.
 */
export async function queryActiveContracts(
    client: Client,
    party: string,
    activeAtOffset: number,
): Promise<ActiveContract[]> {
    const eventFormat: components["schemas"]["EventFormat"] = {
        filtersByParty: {
            [party]: {
                cumulative: [{
                    identifierFilter: {
                        WildcardFilter: {value: {includeCreatedEventBlob: true}},
                    },
                }],
            },
        },
    };


    const resp = await client.POST("/v2/state/active-contracts", {
        body: {activeAtOffset, eventFormat},
    });
    const entries = valueOrError(resp);

    return entries
        .filter((e: components["schemas"]["JsGetActiveContractsResponse"]) =>
            e.contractEntry && "JsActiveContract" in e.contractEntry,
        )
        .map((e: components["schemas"]["JsGetActiveContractsResponse"]) => {
            const ac = (e.contractEntry as {
                JsActiveContract: components["schemas"]["JsActiveContract"]
            }).JsActiveContract;
            return {
                contractId: ac.createdEvent.contractId,
                templateId: ac.createdEvent.templateId,
                synchronizerId: ac.synchronizerId,
                createArgument: ac.createdEvent.createArgument as Record<string, unknown>,
                createdEventBlob: ac.createdEvent.createdEventBlob,
            };
        });
}

/**
 * Find a specific contract in the ACS by template suffix and synchronizer.
 */
export function findContract(
    contracts: ActiveContract[],
    templateSuffix: string,
    synchronizerId?: string,
): ActiveContract | undefined {
    return contracts.find((c) =>
        c.templateId.endsWith(templateSuffix)
        && (synchronizerId === undefined || c.synchronizerId === synchronizerId),
    );
}


