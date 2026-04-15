// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Interactive submission: prepare, sign, and execute commands.

import crypto from "node:crypto";
import { components } from "../generated/api/ledger-api";
import { Client, valueOrError } from "./client";
import { ExternalKeyPair } from "./parties";
import { signHash } from "./signing";

/** A party + its key pair, used for multi-party signing. */
export interface PartySigner {
    partyId: string;
    keyPair: ExternalKeyPair;
}

export async function prepareAndExecute(params: {
    client: Client;
    commands: components["schemas"]["Command"][];
    actAs: string[];
    userId: string;
    commandId: string;
    synchronizerId: string;
    /** All external parties that must sign. Each entry = one party + key pair. */
    signers: PartySigner[];
    disclosedContracts?: components["schemas"]["DisclosedContract"][];
}): Promise<components["schemas"]["ExecuteSubmissionAndWaitResponse"]> {
    // Prepare
    const prepResp = await params.client.POST("/v2/interactive-submission/prepare", {
        body: {
            commandId: params.commandId,
            commands: params.commands,
            actAs: params.actAs,
            userId: params.userId,
            synchronizerId: params.synchronizerId,
            packageIdSelectionPreference: [],
            disclosedContracts: params.disclosedContracts,
        },
    });
    const prepared = valueOrError(prepResp);

    // Each external actAs party must sign the prepared transaction hash
    const partySignatures = params.signers.map((signer) => {
        const fingerprint = signer.keyPair.fingerprint;
        if (fingerprint === undefined) {
            throw new Error(`Missing key fingerprint for signer ${signer.partyId}`);
        }
        const sig = signHash(
            prepared.preparedTransactionHash,
            signer.keyPair.privateKeyObject,
            fingerprint,
        );
        return { party: signer.partyId, signatures: [sig] };
    });

    // Execute
    const execResp = await params.client.POST("/v2/interactive-submission/executeAndWait", {
        body: {
            preparedTransaction: prepared.preparedTransaction,
            hashingSchemeVersion: prepared.hashingSchemeVersion,
            partySignatures: { signatures: partySignatures },
            deduplicationPeriod: {
                DeduplicationDuration: { value: { seconds: 600, nanos: 0 } },
            },
            submissionId: crypto.randomUUID(),
            userId: params.userId,
        },
    });
    return valueOrError(execResp);
}

