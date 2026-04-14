// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import Table from "cli-table3";

export function logSection(title: string) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`  ${title}`);
    console.log(`${"=".repeat(60)}\n`);
}

export function logStep(msg: string) {
    console.log(`  → ${msg}`);
}

export function logOk(msg: string) {
    console.log(`  ✓ ${msg}`);
}

export function logError(msg: string) {
    console.log(`  ✗ ${msg}`);
}

export function showTable<T extends Table.CellValue>(headers: string[], rows: T[][]) {
    const table = new Table({ head: headers });
    rows.forEach((r) => table.push(r));
    console.log(table.toString());
}

