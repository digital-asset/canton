// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import createClient from "openapi-fetch";
import { paths } from "../generated/api/ledger-api";

export function makeClient(baseUrl: string) {
    return createClient<paths>({ baseUrl });
}

export type Client = ReturnType<typeof makeClient>;

export function valueOrError<T>(response: { data?: T; error?: unknown }): T {
    if (response.data === undefined) {
        const errorMessage = JSON.stringify(response.error);
        console.error(`Error: ${errorMessage}`);
        throw new Error(errorMessage);
    }
    return response.data;
}

