// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Multi-sync sandbox topology:
//   V1 (sandbox)   -> S1 + S2  HTTP port 6864
//   V2 (pebblebox) -> S1 + S2  HTTP port 7864
//   V3 (sidebox)   -> S2 only  HTTP port 8864

function envOrDefault(name: string, def: string): string {
    return process.env[name] ?? def;
}

export const V1_HOST = envOrDefault("V1_HOST", "localhost");
export const V1_PORT = envOrDefault("V1_PORT", "6864");

export const V2_HOST = envOrDefault("V2_HOST", "localhost");
export const V2_PORT = envOrDefault("V2_PORT", "7864");

export const V3_HOST = envOrDefault("V3_HOST", "localhost");
export const V3_PORT = envOrDefault("V3_PORT", "8864");

export const V1_URL = `http://${V1_HOST}:${V1_PORT}`;
export const V2_URL = `http://${V2_HOST}:${V2_PORT}`;
export const V3_URL = `http://${V3_HOST}:${V3_PORT}`;

