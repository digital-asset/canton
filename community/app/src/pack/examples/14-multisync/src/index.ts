// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Multi-synchronizer example — main entry point.
// Runs all scenarios sequentially.

import path from "node:path";
import { scenario1Allocations } from "./scenario1_allocations";
import { scenario2ManualReassignment } from "./scenario2_manual_reassignment";
import { scenario3AutomaticReassignment } from "./scenario3_automatic_reassignment";
import { scenario4FailedMissingParty } from "./scenario4_failed_missing_party";
import { scenario5FailedMissingVetting } from "./scenario5_failed_missing_vetting";
import { logSection } from "./logging";

async function main() {
    const darPath = process.env.DAR_PATH
        || path.resolve(__dirname, "../model/.daml/dist/model-tests-1.0.0.dar");

    console.log(`Using DAR: ${darPath}`);

    // Scenario 1 uploads the DAR to all participants and synchronizers
    const topology = await scenario1Allocations(darPath);

    await scenario2ManualReassignment(topology);
    await scenario3AutomaticReassignment(topology);
    await scenario4FailedMissingParty(topology);

    // Scenario 5 runs last: unvets the package on S2, attempts reassignment, then re-vets
    await scenario5FailedMissingVetting(topology);

    logSection("All scenarios complete!");
}

main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});

