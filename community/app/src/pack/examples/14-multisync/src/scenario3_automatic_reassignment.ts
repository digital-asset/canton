// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Scenario 3: Automatic reassignment
//  - P1+P2 create an Iou on S1
//  - P1 creates an IouLinked on S2 referencing the Iou
//  - P1 exercises ProcessWithIou on IouLinked targeting S2
//  - V1 automatically reassigns the Iou from S1 to S2
//  - P3 exercises Iou_Inspect on the Iou on S2

import { makeClient } from "./client";
import { V1_URL, V2_URL } from "./environment";
import { prepareAndExecute } from "./interactive-submission";
import { queryActiveContracts, findContract } from "./acs";
import { logSection, logStep, logOk } from "./logging";
import { Topology } from "./scenario1_allocations";

export async function scenario3AutomaticReassignment(topology: Topology): Promise<void> {
    logSection("Scenario 3: Automatic reassignment");

    const { s1Id, s2Id, p1Party, p2Party, p3Party, p1KeyPair, p2KeyPair, p3KeyPair, v1User, v2User } = topology;
    const v1 = makeClient(V1_URL);
    const v2 = makeClient(V2_URL);

    // P1+P2 create an Iou on S1
    logStep("P1+P2 creating Iou (C1) on S1...");
    const c1Result = await prepareAndExecute({
        client: v1,
        commands: [{
            CreateCommand: {
                templateId: "#model-tests:Iou:Iou",
                createArguments: {
                    issuer: p1Party,
                    owner: p2Party,
                    currency: "CHF",
                    amount: "1000.0",
                    observers: [],
                },
            },
        }],
        actAs: [p1Party, p2Party],
        userId: v1User,
        commandId: `create-auto-c1-${Date.now()}`,
        synchronizerId: s1Id,
        signers: [
            { partyId: p1Party, keyPair: p1KeyPair },
            { partyId: p2Party, keyPair: p2KeyPair },
        ],
    });

    const c1Contracts = await queryActiveContracts(v1, p1Party, c1Result.completionOffset);
    const c1 = findContract(c1Contracts, ":Iou:Iou", s1Id);
    if (!c1) {
        throw new Error("Iou (C1) not found in ACS");
    }
    logOk(`Iou created on S1: ${c1.contractId}`);

    // P1 creates IouLinked (C2) on S2 referencing the Iou
    logStep("P1 creating IouLinked (C2) on S2, referencing Iou...");
    const c2Result = await prepareAndExecute({
        client: v1,
        commands: [{
            CreateCommand: {
                templateId: "#model-tests:Iou:IouLinked",
                createArguments: {
                    holder: p1Party,
                    iouRef: c1.contractId,
                    note: "linked-for-auto-reassignment",
                },
            },
        }],
        actAs: [p1Party],
        userId: v1User,
        commandId: `create-auto-c2-${Date.now()}`,
        synchronizerId: s2Id,
        signers: [{ partyId: p1Party, keyPair: p1KeyPair }],
    });

    const c2Contracts = await queryActiveContracts(v1, p1Party, c2Result.completionOffset);
    const c2 = findContract(c2Contracts, ":Iou:IouLinked", s2Id);
    if (!c2) {
        throw new Error("IouLinked (C2) not found in ACS");
    }
    logOk(`IouLinked created on S2: ${c2.contractId}`);

    // P1 exercises ProcessWithIou on C2, targeting S2
    // This triggers auto-reassignment of the Iou from S1 to S2
    logStep("P1 exercising ProcessWithIou on IouLinked → auto-reassignment of Iou...");
    const processResult = await prepareAndExecute({
        client: v1,
        commands: [{
            ExerciseCommand: {
                templateId: "#model-tests:Iou:IouLinked",
                contractId: c2.contractId,
                choice: "ProcessWithIou",
                choiceArgument: {},
            },
        }],
        actAs: [p1Party, p2Party],
        userId: v1User,
        commandId: `process-auto-${Date.now()}`,
        synchronizerId: s2Id,
        signers: [
            { partyId: p1Party, keyPair: p1KeyPair },
            { partyId: p2Party, keyPair: p2KeyPair },
        ],
    });
    logOk(`Auto-reassignment succeeded, updateId: ${processResult.updateId}`);

    // Verify Iou is now on S2
    const afterProcess = await queryActiveContracts(v1, p1Party, processResult.completionOffset);
    const c1OnS2 = afterProcess.find((c) => c.contractId === c1.contractId);
    if (!c1OnS2) {
        throw new Error("Iou not found after auto-reassignment");
    }
    logOk(`Iou verified on ${c1OnS2.synchronizerId} after auto-reassignment`);

    // P3 exercises Iou_Inspect on the Iou via explicit disclosure
    logStep("P3 exercising Iou_Inspect on Iou (now on S2)...");
    const blob = c1OnS2.createdEventBlob;
    if (!blob) {
        throw new Error("No createdEventBlob found for Iou contract in ACS");
    }
    const inspectResult = await prepareAndExecute({
        client: v2,
        commands: [{
            ExerciseCommand: {
                templateId: "#model-tests:Iou:Iou",
                contractId: c1.contractId,
                choice: "Iou_Inspect",
                choiceArgument: { viewer: p3Party },
            },
        }],
        actAs: [p3Party],
        userId: v2User,
        commandId: `inspect-auto-${Date.now()}`,
        synchronizerId: s2Id,
        signers: [{ partyId: p3Party, keyPair: p3KeyPair }],
        disclosedContracts: [{ createdEventBlob: blob, synchronizerId: s2Id }],
    });
    logOk(`P3 exercised Iou_Inspect, updateId: ${inspectResult.updateId}`);

    logOk("Scenario 3 complete!");
}

