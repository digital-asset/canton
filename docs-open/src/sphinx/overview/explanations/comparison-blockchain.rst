..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
   Purpose: This document should help someone who has worked with other systems, eg. like ETH before to understand
   the differences and connect their knowledge to Canton Network concepts.

Comparison with other Ledgers
=============================



Ethereum
********

.. todo:: <https://github.com/DACH-NY/canton/issues/25697>
   * Main difference in property: privacy
   * Difference in stakeholder model (contracts are among specific parties, not global to anyone)
   * Difference in consensus model (stakeholder based agreement vs PoW/PoS)
   * Difference in building transactions due to privacy -- assembled off-ledger by Ledger API client. See https://github.com/global-synchronizer-foundation/cips/blob/main/cip-0056/cip-0056.md#shipping-utxos-on-ledger for rationale of off-ledger assembly. Take discussion from Canton Polyglot https://www.canton.network/hubfs/Canton%20Network%20Files/whitepapers/Polyglot_Canton_Whitepaper_11_02_25.pdf
   * Contract language, different (Solidity vs Daml)
   * Difference in the operational model (one chain, everyone sees every thing) vs multi-chain with selected visibility
   * Difference in governance model (global sync via global governance of linux, otherwise each network and node is independent)
   * Difference in the connectivity model (connect via your pnode, sync vs eth full node)
   * Fact that pnodes do not expose ports / are not directly reachable
   * Difference in scalability model and operational readiness (pruning)
   * Look at Finadium report for a comparison from the fin-tech perspective https://www.canton.network/hubfs/Canton%20Network%20Files/Landing%20Pages/Finadium%20Canton%20and%20Ethereum%20for%20Post%20Trade%20Digital%20Asset.pdf
   * Look at Edward's write-up: https://docs.google.com/document/d/1ciXdLxFgPyfIvc5V4TwA3IeL76IwKWGNBbxjBeChWRQ/edit?tab=t.0
