// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Scenario 2: Manual reassignment
//  - P1+P2 create an Iou contract on S1
//  - P1 unassigns the Iou from S1
//  - P1 assigns the Iou to S2
//  - P3 exercises Iou_Inspect on the Iou via explicit disclosure

import { makeClient } from "./client";
import { V1_URL, V2_URL } from "./environment";
import { prepareAndExecute } from "./interactive-submission";
import { submitUnassign, submitAssign, extractReassignmentId } from "./reassignment";
import { queryActiveContracts, findContract } from "./acs";
import { logSection, logStep, logOk } from "./logging";
import { Topology } from "./scenario1_allocations";

export async function scenario2ManualReassignment(topology: Topology): Promise<void> {
    logSection("Scenario 2: Manual reassignment");

    const { s1Id, s2Id, p1Party, p2Party, p3Party, p1KeyPair, p2KeyPair, p3KeyPair, v1User, v2User } = topology;
    const v1 = makeClient(V1_URL);
    const v2 = makeClient(V2_URL);

    // P1+P2 create an Iou on S1
    logStep("P1+P2 creating Iou on S1...");
    const createResult = await prepareAndExecute({
        client: v1,
        commands: [{
            CreateCommand: {
                templateId: "#model-tests:Iou:Iou",
                createArguments: {
                    issuer: p1Party,
                    owner: p2Party,
                    currency: "EUR",
                    amount: "500.0",
                    observers: [],
                },
            },
        }],
        actAs: [p1Party, p2Party],
        userId: v1User,
        commandId: `create-manual-${Date.now()}`,
        synchronizerId: s1Id,
        signers: [
            { partyId: p1Party, keyPair: p1KeyPair },
            { partyId: p2Party, keyPair: p2KeyPair },
        ],
    });
    logOk(`Iou created, offset: ${createResult.completionOffset}`);

    // Find the contract in ACS
    const contracts = await queryActiveContracts(v1, p1Party, createResult.completionOffset);
    const c1 = findContract(contracts, ":Iou:Iou", s1Id);
    if (!c1) {
        throw new Error("Iou not found in ACS after creation");
    }
    logOk(`C1 contractId: ${c1.contractId}`);

    // Extract createdEventBlob from ACS for later explicit disclosure to P3
    const createdEventBlob = c1.createdEventBlob;
    if (!createdEventBlob) {
        throw new Error("No createdEventBlob found for Iou contract in ACS");
    }
    logOk(`Got createdEventBlob (length: ${createdEventBlob.length})`);

    // P1 unassigns C1 from S1 (targeting S2)
    logStep("P1 unassigning Iou from S1 → S2...");
    const unassignResult = await submitUnassign(v1, c1.contractId, s1Id, s2Id, p1Party, v1User);
    const reassignmentId = extractReassignmentId(unassignResult);
    if (!reassignmentId) {
        throw new Error("Failed to extract reassignmentId from unassign response");
    }
    logOk(`Unassigned, reassignmentId: ${reassignmentId}`);

    // P1 assigns C1 to S2
    logStep("P1 assigning Iou to S2...");
    const assignResult = await submitAssign(v1, reassignmentId, s1Id, s2Id, p1Party, v1User);
    logOk(`Assigned to S2, updateId: ${assignResult.reassignment.updateId}`);

    // Verify contract is now on S2
    const afterAssign = await queryActiveContracts(v1, p1Party, assignResult.reassignment.offset);
    const c1OnS2 = afterAssign.find((c) => c.contractId === c1.contractId);
    if (!c1OnS2) {
        throw new Error("Iou not found in ACS after assignment");
    }
    logOk(`Iou verified on ${c1OnS2.synchronizerId}`);

    // P3 exercises Iou_Inspect on C1 via explicit disclosure through V2
    logStep("P3 exercising Iou_Inspect on Iou via explicit disclosure...");
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
        commandId: `inspect-manual-${Date.now()}`,
        synchronizerId: s2Id,
        signers: [{ partyId: p3Party, keyPair: p3KeyPair }],
        disclosedContracts: [{ createdEventBlob, synchronizerId: s2Id }],
    });
    logOk(`P3 exercised Iou_Inspect, updateId: ${inspectResult.updateId}`);

    logOk("Scenario 2 complete!");
}

