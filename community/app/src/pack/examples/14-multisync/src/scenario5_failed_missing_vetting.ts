// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Scenario 5: Failed reassignment due to missing vetting
//  - Unvet the model-tests package on S2 via /v2/package-vetting POST
//  - P1+P2 create an Iou contract on S1
//  - P1 tries to unassign the Iou from S1 to S2
//  - Unassign fails because the Iou package is not vetted on S2
//  - Re-vet the package on S2 to restore topology

import { makeClient } from "./client";
import { V1_URL, V2_URL } from "./environment";
import { unvetPackage, vetPackage } from "./packages";
import { prepareAndExecute } from "./interactive-submission";
import { submitUnassign } from "./reassignment";
import { queryActiveContracts, findContract } from "./acs";
import { logSection, logStep, logOk } from "./logging";
import { Topology } from "./scenario1_allocations";

const PACKAGE_NAME = "model-tests";

export async function scenario5FailedMissingVetting(
    topology: Topology,
): Promise<void> {
    logSection("Scenario 5: Failed reassignment — missing vetting");

    const { s1Id, s2Id, p1Party, p2Party, p1KeyPair, p2KeyPair, v1User } = topology;
    const v1 = makeClient(V1_URL);
    const v2 = makeClient(V2_URL);

    // Unvet the model-tests package on S2 for V1 and V2
    logStep("Unvetting model-tests package on S2 for V1 and V2...");
    await unvetPackage(v1, s2Id, PACKAGE_NAME);
    logOk("Unvetted on V1/S2");
    await unvetPackage(v2, s2Id, PACKAGE_NAME);
    logOk("Unvetted on V2/S2");

    try {
        // P1+P2 create an Iou contract on S1 (package is still vetted on S1)
        logStep("P1+P2 creating Iou contract on S1...");
        const createResult = await prepareAndExecute({
            client: v1,
            commands: [{
                CreateCommand: {
                    templateId: "#model-tests:Iou:Iou",
                    createArguments: {
                        issuer: p1Party,
                        owner: p2Party,
                        currency: "USD",
                        amount: "100.0",
                        observers: [],
                    },
                },
            }],
            actAs: [p1Party, p2Party],
            userId: v1User,
            commandId: `create-vetting-test-${Date.now()}`,
            synchronizerId: s1Id,
            signers: [
                { partyId: p1Party, keyPair: p1KeyPair },
                { partyId: p2Party, keyPair: p2KeyPair },
            ],
        });

        const contracts = await queryActiveContracts(v1, p1Party, createResult.completionOffset);
        const iou = findContract(contracts, ":Iou:Iou", s1Id);
        if (!iou) {
            throw new Error("Iou contract not found after creation");
        }
        logOk(`Iou created on S1: ${iou.contractId}`);

        // P1 tries to unassign from S1 → S2 — should fail (package not vetted on S2)
        logStep("P1 attempting to unassign Iou from S1 → S2 (expected to fail)...");
        try {
            await submitUnassign(v1, iou.contractId, s1Id, s2Id, p1Party, v1User);
            throw new Error("Unassign unexpectedly succeeded; expected failure because package is not vetted on S2");
        } catch (error: unknown) {
            const fullMsg = error instanceof Error ? error.message : String(error);
            const msg = fullMsg.substring(0, 200);
            if (!fullMsg.includes("has not vetted")) {
                throw new Error(`Unassign failed for an unexpected reason: ${msg}`);
            }
            logOk(`Unassign correctly rejected: ${msg}`);
        }
    } finally {
        // Re-vet the package on S2 to restore topology
        logStep("Re-vetting model-tests package on S2 for V1 and V2...");
        await vetPackage(v1, s2Id, PACKAGE_NAME);
        logOk("Re-vetted on V1/S2");
        await vetPackage(v2, s2Id, PACKAGE_NAME);
        logOk("Re-vetted on V2/S2");
    }

    logOk("Scenario 5 complete!");
}

