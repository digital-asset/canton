# Canton Architecture Overview

This page gives a high-level view of key components and how they interact.

```mermaid
flowchart LR
  subgraph Domain["Synchronization Domain"]
    SQ[Sequencer]:::comp
    MED[Mediator]:::comp
  end

  PN1[Participant Node A]:::node -->|Authenticated messages| SQ
  PN2[Participant Node B]:::node -->|Authenticated messages| SQ

  %% The mediator initiates the connection to the sequencer
  MED -->|Initiates connection| SQ
  %% Once established, messages flow bidirectionally
  MED <--> SQ

  classDef comp fill:#eee,stroke:#999,color:#000;
  classDef node fill:#f7f7f7,stroke:#777,color:#000;
```

**Notes**
- Participant nodes do **not** communicate directly; they interact through the sequencer.
- The **mediator** initiates the connection to the **sequencer**; once established, the flow becomes bidirectional.
- The **Topology Manager** is no longer part of Canton 3.x and is therefore omitted.
