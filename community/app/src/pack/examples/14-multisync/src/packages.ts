// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package management: DAR upload and package vetting.

import * as fs from "node:fs/promises";

import { components } from "../generated/api/ledger-api";
import { Client, valueOrError } from "./client";
import { logOk } from "./logging";

// ----- Package vetting -----

/**
 * List vetted packages on a participant, optionally filtered by synchronizer.
 */
export async function listVettedPackages(
    client: Client,
    synchronizerId?: string,
): Promise<components["schemas"]["VettedPackages"][]> {
    const body: components["schemas"]["ListVettedPackagesRequest"] = synchronizerId
        ? { topologyStateFilter: { synchronizerIds: [synchronizerId] } }
        : {};
    const resp = await client.POST("/v2/package-vetting/list", { body });
    return valueOrError(resp).vettedPackages ?? [];
}

/**
 * Unvet a package by name on a specific synchronizer.
 */
export async function unvetPackage(
    client: Client,
    synchronizerId: string,
    packageName: string,
): Promise<void> {
    const resp = await client.POST("/v2/package-vetting/update", {
        body: {
            synchronizerId,
            changes: [{
                operation: {
                    Unvet: {
                        value: {
                            packages: [{ packageName }],
                        },
                    },
                },
            }],
        },
    });
    valueOrError(resp);
}

/**
 * Re-vet a package by name on a specific synchronizer.
 */
export async function vetPackage(
    client: Client,
    synchronizerId: string,
    packageName: string,
): Promise<void> {
    const resp = await client.POST("/v2/package-vetting/update", {
        body: {
            synchronizerId,
            changes: [{
                operation: {
                    Vet: {
                        value: {
                            packages: [{ packageName }],
                        },
                    },
                },
            }],
        },
    });
    valueOrError(resp);
}

// ----- DAR upload -----

export async function uploadDar(
    client: Client,
    darPath: string,
    synchronizerId: string,
): Promise<void> {
    const darBuffer = await fs.readFile(darPath);

    const resp = await client.POST("/v2/dars", {
        params: { query: { synchronizerId, vetAllPackages: true } },
        body: darBuffer.toString("base64"),
        bodySerializer: () => darBuffer,
        headers: { "Content-Type": "application/octet-stream" },
    });
    if (resp.error) {
        throw new Error(`DAR upload failed: ${JSON.stringify(resp.error)}`);
    }
}

/** A named participant client used for DAR vetting. */
export interface Participant {
    label: string;
    client: Client;
}

/**
 * Upload and vet a DAR on every (participant, synchronizer) combination.
 *
 * This makes the vetting topology explicit in one place:
 *
 * ```ts
 * await uploadDarToAll(darPath, [v1, v2], [s1Id, s2Id]);
 * await uploadDarToAll(darPath, [v3],     [s2Id]);
 * ```
 */
export async function uploadDarToAll(
    darPath: string,
    participants: Participant[],
    synchronizerIds: { label: string; id: string }[],
): Promise<void> {
    for (const p of participants) {
        for (const s of synchronizerIds) {
            await uploadDar(p.client, darPath, s.id);
            logOk(`DAR uploaded on ${p.label} for ${s.label}`);
        }
    }
}

