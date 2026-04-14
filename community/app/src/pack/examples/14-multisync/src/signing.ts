// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Ed25519 signing helper shared by party allocation and interactive submission.

import crypto from "node:crypto";
import { components } from "../generated/api/ledger-api";

export function signHash(
    hashBase64: string,
    privateKey: crypto.KeyObject,
    fingerprint: string,
): components["schemas"]["Signature"] {
    const hashBytes = Buffer.from(hashBase64, "base64");
    const signature = crypto.sign(null, hashBytes, privateKey);
    return {
        format: "SIGNATURE_FORMAT_RAW",
        signature: signature.toString("base64"),
        signedBy: fingerprint,
        signingAlgorithmSpec: "SIGNING_ALGORITHM_SPEC_ED25519",
    };
}

