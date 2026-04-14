// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Scenario 1: Allocations
//  - Discover connected synchronizers on V1
//  - Allocate external parties P1 on S1+S2, P2 on S1+S2, P3 on S2
//  - Check connected synchronizers for parties
//  - Upload and vet packages on all participants and synchronizers
//  - Create users on participants

import { makeClient } from "./client";
import { V1_URL, V2_URL, V3_URL } from "./environment";
import { getConnectedSynchronizers } from "./state";
import { createUser, makeCanActAs, makeCanReadAs } from "./users";
import { uploadDarToAll } from "./packages";
import {
    generateEd25519KeyPair,
    allocateExternalParty,
    type ExternalKeyPair,
} from "./parties";
import { logSection, logStep, logOk, showTable } from "./logging";

export interface Topology {
    darPath: string;
    s1Id: string;
    s2Id: string;
    p1Party: string;
    p2Party: string;
    p3Party: string;
    p1KeyPair: ExternalKeyPair;
    p2KeyPair: ExternalKeyPair;
    p3KeyPair: ExternalKeyPair;
    v1User: string;
    v2User: string;
    v3User: string;
}

export async function scenario1Allocations(darPath: string): Promise<Topology> {
    logSection("Scenario 1: Allocations");

    const v1 = makeClient(V1_URL);
    const v2 = makeClient(V2_URL);
    const v3 = makeClient(V3_URL);

    // 1. Check connected synchronizers on V1
    logStep("Checking connected synchronizers on V1 (sandbox)...");
    const v1Syncs = await getConnectedSynchronizers(v1);
    showTable(
        ["Alias", "Synchronizer ID"],
        v1Syncs.map((s) => [s.synchronizerAlias, s.synchronizerId]),
    );

    const s1 = v1Syncs.find((s) => s.synchronizerAlias === "synchronizer-1");
    const s2 = v1Syncs.find((s) => s.synchronizerAlias === "synchronizer-2");
    if (!s1 || !s2) {
        throw new Error("Expected synchronizer-1 and synchronizer-2 on V1");
    }
    const s1Id = s1.synchronizerId;
    const s2Id = s2.synchronizerId;
    logOk(`S1=${s1Id}, S2=${s2Id}`);

    // Verify V2 sees the same synchronizers
    logStep("Checking connected synchronizers on V2 (pebblebox)...");
    const v2Syncs = await getConnectedSynchronizers(v2);
    logOk(`V2 connected to ${v2Syncs.length} synchronizer(s): ${v2Syncs.map((s) => s.synchronizerAlias).join(", ")}`);

    // Verify V3 sees only S2
    logStep("Checking connected synchronizers on V3 (sidebox)...");
    const v3Syncs = await getConnectedSynchronizers(v3);
    logOk(`V3 connected to ${v3Syncs.length} synchronizer(s): ${v3Syncs.map((s) => s.synchronizerAlias).join(", ")}`);

    // 2. Allocate external parties
    logStep("Generating Ed25519 key pairs for external parties...");
    const p1KeyPair = generateEd25519KeyPair();
    const p2KeyPair = generateEd25519KeyPair();
    const p3KeyPair = generateEd25519KeyPair();

    logStep("Allocating P1 as external party on S1 via V1...");
    const p1OnS1 = await allocateExternalParty(v1, s1Id, "P1", p1KeyPair, "");
    logOk(`P1 allocated: ${p1OnS1.partyId}`);

    logStep("Allocating P1 on S2 via V1...");
    await allocateExternalParty(v1, s2Id, "P1", p1KeyPair, "");
    logOk("P1 present on S1 and S2");

    logStep("Allocating P2 as external party on S1 via V2...");
    const p2OnS1 = await allocateExternalParty(v2, s1Id, "P2", p2KeyPair, "");
    logOk(`P2 allocated: ${p2OnS1.partyId}`);

    logStep("Allocating P2 on S2 via V2...");
    await allocateExternalParty(v2, s2Id, "P2", p2KeyPair, "");
    logOk("P2 present on S1 and S2");

    logStep("Allocating P3 as external party on S2 via V3...");
    const p3OnS2 = await allocateExternalParty(v3, s2Id, "P3", p3KeyPair, "");
    logOk(`P3 allocated: ${p3OnS2.partyId} (S2 only)`);

    const p1Party = p1OnS1.partyId;
    const p2Party = p2OnS1.partyId;
    const p3Party = p3OnS2.partyId;

    // 3. Check connected synchronizers for parties
    logStep("Checking connected synchronizers for P1 through V1...");
    const p1Syncs = await getConnectedSynchronizers(v1, p1Party);
    logOk(`P1 via V1: ${p1Syncs.map((s) => s.synchronizerAlias).join(", ")}`);

    logStep("Checking connected synchronizers for P2 through V2...");
    const p2Syncs = await getConnectedSynchronizers(v2, p2Party);
    logOk(`P2 via V2: ${p2Syncs.map((s) => s.synchronizerAlias).join(", ")}`);

    // 4. Upload and vet packages on ALL participants and synchronizers
    //
    //  DAR vetting topology:
    //    V1 (sandbox)   → S1 + S2
    //    V2 (pebblebox) → S1 + S2
    //    V3 (sidebox)   → S2
    //
    logStep("Uploading DAR to V1, V2 for S1+S2 and V3 for S2...");
    await uploadDarToAll(darPath,
        [{ label: "V1", client: v1 }, { label: "V2", client: v2 }],
        [{ label: "S1", id: s1Id }, { label: "S2", id: s2Id }],
    );
    await uploadDarToAll(darPath,
        [{ label: "V3", client: v3 }],
        [{ label: "S2", id: s2Id }],
    );

    // 5. Create users on each participant
    const v1User = "v1-user";
    const v2User = "v2-user";
    const v3User = "v3-user";

    logStep("Creating users on participants...");
    await createUser(v1, v1User, p1Party, [
        makeCanActAs(p1Party),
        makeCanActAs(p2Party),
    ]);
    await createUser(v2, v2User, p2Party, [
        makeCanActAs(p1Party),
        makeCanActAs(p2Party),
        makeCanActAs(p3Party),
    ]);
    await createUser(v3, v3User, p3Party, [
        makeCanActAs(p1Party),
        makeCanActAs(p3Party),
    ]);
    logOk("Users created on V1, V2, V3");

    logOk("Scenario 1 complete!");

    return {
        darPath, s1Id, s2Id,
        p1Party, p2Party, p3Party,
        p1KeyPair, p2KeyPair, p3KeyPair,
        v1User, v2User, v3User,
    };
}

