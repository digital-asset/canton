// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Reassignment operations: unassign and assign contracts between synchronizers.

import { components } from "../generated/api/ledger-api";
import { Client, valueOrError } from "./client";

export async function submitUnassign(
    client: Client,
    contractId: string,
    source: string,
    target: string,
    submitter: string,
    userId: string,
): Promise<components["schemas"]["JsSubmitAndWaitForReassignmentResponse"]> {
    const resp = await client.POST("/v2/commands/submit-and-wait-for-reassignment", {
        body: {
            reassignmentCommands: {
                submitter,
                commandId: `unassign-${Date.now()}`,
                userId,
                commands: [{
                    command: {
                        UnassignCommand: {
                            value: { contractId, source, target },
                        },
                    },
                }],
            },
            eventFormat: {
                filtersByParty: { [submitter]: {} },
            },
        },
    });
    return valueOrError(resp);
}

export async function submitAssign(
    client: Client,
    reassignmentId: string,
    source: string,
    target: string,
    submitter: string,
    userId: string,
): Promise<components["schemas"]["JsSubmitAndWaitForReassignmentResponse"]> {
    const resp = await client.POST("/v2/commands/submit-and-wait-for-reassignment", {
        body: {
            reassignmentCommands: {
                submitter,
                commandId: `assign-${Date.now()}`,
                userId,
                commands: [{
                    command: {
                        AssignCommand: {
                            value: { reassignmentId, source, target },
                        },
                    },
                }],
            },
            eventFormat: {
                filtersByParty: { [submitter]: {} },
            },
        },
    });
    return valueOrError(resp);
}

/**
 * Extract the reassignmentId from an unassign response.
 */
export function extractReassignmentId(
    response: components["schemas"]["JsSubmitAndWaitForReassignmentResponse"],
): string | undefined {
    for (const event of response.reassignment.events) {
        if ("JsUnassignedEvent" in event) {
            return event.JsUnassignedEvent.value.reassignmentId;
        }
    }
    return undefined;
}

