# Multi-Synchronizer Example

## Overview

This example demonstrates multi-synchronizer operations with Canton using the JSON API and TypeScript.

It uses a Canton sandbox with the `--multi-sync` flag, which starts:
- **3 participants**: `sandbox` (V1), `pebblebox` (V2), `sidebox` (V3)
- **2 synchronizers**: `synchronizer-1` (S1), `synchronizer-2` (S2)
- **Topology**: V1->S1+S2, V2->S1+S2, V3->S2 only

All parties are external parties using Ed25519 key pairs and interactive (prepare+execute) submission.

## Prerequisites

- Canton software (usually this example is a part of canton release)
- Node.js (v18+)
- Daml SDK (`dpm` available in PATH)

## Running

### Terminal 1: Start Canton

```bash
./run.sh
```

Wait until both synchronizers are visible on V1.

### Terminal 2: Run the TypeScript scenarios

```bash
npm install
npm run build
npm run scenario
```

## Scenarios

1. **Allocations** — Discover synchronizers, allocate external parties across synchronizers, upload and vet DAR on all participants and synchronizers, create users
2. **Manual reassignment** — Create an Iou on S1, manually unassign+assign it to S2, exercise Iou_Inspect via explicit disclosure
3. **Automatic reassignment** — Create an Iou on S1 and an IouLinked on S2, trigger auto-reassignment via cross-sync fetch, then inspect on S2
4. **Failed reassignment (missing party)** — Attempt to reassign an Iou to a synchronizer where a signatory's participant is absent
5. **Failed reassignment (missing vetting)** — Unvet the package on S2 via the package-vetting API, attempt to reassign an Iou whose package is not vetted on the target synchronizer, then re-vet the package


