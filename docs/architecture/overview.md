# Canton Architecture Overview

This page gives a high-level view of key components and how they interact.

```mermaid
flowchart LR
  subgraph Domain["Synchronization Domain"]
    SQ[Sequencer]:::comp
    MED[Mediator]:::comp
    TM[Topology Mgmt]:::comp
  end

  PN1[Participant Node A]:::node -->|Authenticated messages| SQ
  PN2[Participant Node B]:::node -->|Authenticated messages| SQ

  SQ --> MED
  MED --> SQ

  subgraph Ext["Other Domain / External Ledger"]
    X1[Participant Node / Bridge]
  end

  PN1 <--> PN2
  SQ <--> X1

  classDef comp fill:#eee,stroke:#999,color:#000;
  classDef node fill:#f7f7f7,stroke:#777,color:#000;
