// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Scenario 4: Failed reassignment due to missing party
//  - P1+P3 create an Iou on S2
//  - P1 tries to unassign the Iou from S2 to S1
//  - Unassign fails because P3's participant (V3/sidebox) is not connected to S1

import { makeClient } from "./client";
import { V1_URL } from "./environment";
import { prepareAndExecute } from "./interactive-submission";
import { submitUnassign } from "./reassignment";
import { queryActiveContracts } from "./acs";
import { logSection, logStep, logOk } from "./logging";
import { Topology } from "./scenario1_allocations";

export async function scenario4FailedMissingParty(topology: Topology): Promise<void> {
    logSection("Scenario 4: Failed reassignment — missing party");

    const { s1Id, s2Id, p1Party, p3Party, p1KeyPair, p3KeyPair, v1User } = topology;
    const v1 = makeClient(V1_URL);

    // P1+P3 create an Iou on S2
    logStep("P1+P3 creating Iou on S2...");
    const createResult = await prepareAndExecute({
        client: v1,
        commands: [{
            CreateCommand: {
                templateId: "#model-tests:Iou:Iou",
                createArguments: {
                    issuer: p1Party,
                    owner: p3Party,
                    currency: "GBP",
                    amount: "250.0",
                    observers: [],
                },
            },
        }],
        actAs: [p1Party, p3Party],
        userId: v1User,
        commandId: `create-missing-party-${Date.now()}`,
        synchronizerId: s2Id,
        signers: [
            { partyId: p1Party, keyPair: p1KeyPair },
            { partyId: p3Party, keyPair: p3KeyPair },
        ],
    });

    const contracts = await queryActiveContracts(v1, p1Party, createResult.completionOffset);
    const c1 = contracts.find((c) =>
        c.templateId.endsWith(":Iou:Iou")
        && c.synchronizerId === s2Id
        && c.createArgument?.currency === "GBP",
    );
    if (!c1) {
        throw new Error("Iou not found after creation");
    }
    logOk(`Iou created on S2: ${c1.contractId}`);

    // P1 tries to unassign the Iou from S2 → S1
    // This should fail because P3 is only on S2 (V3/sidebox is not connected to S1)
    logStep("P1 attempting to unassign Iou from S2 → S1 (expected to fail)...");
    try {
        await submitUnassign(v1, c1.contractId, s2Id, s1Id, p1Party, v1User);
        throw new Error("Unassign unexpectedly succeeded; expected failure because P3 is not active on S1");
    } catch (error: unknown) {
        const fullMsg = error instanceof Error ? error.message : String(error);
        const msg = fullMsg.substring(0, 200);
        if (!fullMsg.includes("not active on the target synchronizer")) {
            throw new Error(`Unassign failed for an unexpected reason: ${msg}`);
        }
        logOk(`Unassign correctly rejected: ${msg}`);
    }

    logOk("Scenario 4 complete!");
}

