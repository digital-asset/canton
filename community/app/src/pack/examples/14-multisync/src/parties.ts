// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// External party allocation helpers.
// External parties own their own signing keys (Ed25519) and must sign all transactions.

import crypto from "node:crypto";
import { components } from "../generated/api/ledger-api";
import { Client, valueOrError } from "./client";
import { signHash } from "./signing";

// ----- Key generation -----

export interface ExternalKeyPair {
    publicKey: components["schemas"]["SigningPublicKey"];
    privateKeyObject: crypto.KeyObject;
    fingerprint?: string; // populated after generateTopology
}

export function generateEd25519KeyPair(): ExternalKeyPair {
    const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
    const publicKeyDer = publicKey.export({ type: "spki", format: "der" });

    return {
        publicKey: {
            format: "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
            keyData: publicKeyDer.toString("base64"),
            keySpec: "SIGNING_KEY_SPEC_EC_CURVE25519",
        },
        privateKeyObject: privateKey,
    };
}



// ----- Generate topology + allocate -----

export async function allocateExternalParty(
    client: Client,
    synchronizerId: string,
    partyHint: string,
    keyPair: ExternalKeyPair,
    userId: string,
): Promise<{ partyId: string; fingerprint: string }> {
    // 1. Generate topology transactions
    const topoResp = await client.POST("/v2/parties/external/generate-topology", {
        body: {
            synchronizer: synchronizerId,
            partyHint,
            publicKey: keyPair.publicKey,
        },
    });
    const topo = valueOrError(topoResp);

    const fingerprint = topo.publicKeyFingerprint;
    keyPair.fingerprint = fingerprint;

    // 2. Sign the multi-hash
    const multiHashSig = signHash(topo.multiHash, keyPair.privateKeyObject, fingerprint);

    // 3. Build signed topology transactions
    const onboardingTransactions = topo.topologyTransactions.map((tx: string) => ({ transaction: tx }));

    // 4. Allocate
    const allocResp = await client.POST("/v2/parties/external/allocate", {
        body: {
            synchronizer: synchronizerId,
            onboardingTransactions,
            multiHashSignatures: [multiHashSig],
            waitForAllocation: true,
            userId,
        },
    });
    const alloc = valueOrError(allocResp);

    return { partyId: alloc.partyId, fingerprint };
}
